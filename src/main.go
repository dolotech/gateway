package main

import (
	"encoding/binary"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	. "types"
	"utils"

	log "github.com/Sirupsen/logrus"
	"github.com/xtaci/kcp-go"
	"gopkg.in/urfave/cli.v2"
	"services"
	"misc/packet"
	"sync"
	"os/signal"
	"syscall"
	"pb"
	"client_handler"
	"errors"
)

type Config struct {
	listen                        string
	readDeadline                  time.Duration
	sockbuf                       int
	udp_sockbuf                   int
	txqueuelen                    int
	dscp                          int
	sndwnd                        int
	rcvwnd                        int
	mtu                           int
	nodelay, interval, resend, nc int
}

func main() {
	log.SetLevel(log.DebugLevel)

	// to catch all uncaught panic
	defer utils.PrintPanicStack()

	// open profiling
	go http.ListenAndServe("0.0.0.0:6060", nil)
	app := &cli.App{
		Name:    "agent",
		Usage:   "a gateway for games with stream multiplexing",
		Version: "2.0",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "listen",
				Value: ":8888",
				Usage: "listening address:port",
			},
			&cli.StringSliceFlag{
				Name:  "etcd-hosts",
				Value: cli.NewStringSlice("http://127.0.0.1:2379"),
				Usage: "etcd hosts",
			},
			&cli.StringFlag{
				Name:  "etcd-root",
				Value: "/backends",
				Usage: "etcd root path",
			},
			&cli.StringSliceFlag{
				Name:  "services",
				Value: cli.NewStringSlice("snowflake-10000", "game-10000"),
				Usage: "auto-discovering services",
			},
			&cli.DurationFlag{
				Name:  "read-deadline",
				Value: 15 * time.Second,
				Usage: "per connection read timeout",
			},
			&cli.IntFlag{
				Name:  "txqueuelen",
				Value: 128,
				Usage: "per connection output message queue, packet will be dropped if exceeds",
			},
			&cli.IntFlag{
				Name:  "sockbuf",
				Value: 32767,
				Usage: "per connection tcp socket buffer",
			},
			&cli.IntFlag{
				Name:  "udp-sockbuf",
				Value: 4194304,
				Usage: "UDP listener socket buffer",
			},
			&cli.IntFlag{
				Name:  "udp-sndwnd",
				Value: 32,
				Usage: "per connection UDP send window",
			},
			&cli.IntFlag{
				Name:  "udp-rcvwnd",
				Value: 32,
				Usage: "per connection UDP recv window",
			},
			&cli.IntFlag{
				Name:  "udp-mtu",
				Value: 1280,
				Usage: "MTU of UDP packets, without IP(20) + UDP(8)",
			},
			&cli.IntFlag{
				Name:  "dscp",
				Value: 46,
				Usage: "set DSCP(6bit)",
			},
			&cli.IntFlag{
				Name:  "nodelay",
				Value: 1,
				Usage: "ikcp_nodelay()",
			},
			&cli.IntFlag{
				Name:  "interval",
				Value: 20,
				Usage: "ikcp_nodelay()",
			},
			&cli.IntFlag{
				Name:  "resend",
				Value: 1,
				Usage: "ikcp_nodelay()",
			},
			&cli.IntFlag{
				Name:  "nc",
				Value: 1,
				Usage: "ikcp_nodelay()",
			},
			&cli.IntFlag{
				Name:  "rpm-limit",
				Value: 200,
				Usage: "per connection rpm limit",
			},
		},
		Action: func(c *cli.Context) error {
			log.Println("listen:", c.String("listen"))
			log.Println("etcd-hosts:", c.StringSlice("etcd-hosts"))
			log.Println("etcd-root:", c.String("etcd-root"))
			log.Println("services:", c.StringSlice("services"))
			log.Println("read-deadline:", c.Duration("read-deadline"))
			log.Println("txqueuelen:", c.Int("txqueuelen"))
			log.Println("sockbuf:", c.Int("sockbuf"))
			log.Println("udp-sockbuf:", c.Int("udp-sockbuf"))
			log.Println("udp-sndwnd:", c.Int("udp-sndwnd"))
			log.Println("udp-rcvwnd:", c.Int("udp-rcvwnd"))
			log.Println("udp-mtu:", c.Int("udp-mtu"))
			log.Println("dscp:", c.Int("dscp"))
			log.Println("rpm-limit:", c.Int("rpm-limit"))
			log.Println("nodelay parameters:", c.Int("nodelay"), c.Int("interval"), c.Int("resend"), c.Int("nc"))

			//setup net param
			config := &Config{
				listen:       c.String("listen"),
				readDeadline: c.Duration("read-deadline"),
				sockbuf:      c.Int("sockbuf"),
				udp_sockbuf:  c.Int("udp-sockbuf"),
				txqueuelen:   c.Int("txqueuelen"),
				dscp:         c.Int("dscp"),
				sndwnd:       c.Int("udp-sndwnd"),
				rcvwnd:       c.Int("udp-rcvwnd"),
				mtu:          c.Int("udp-mtu"),
				nodelay:      c.Int("nodelay"),
				interval:     c.Int("interval"),
				resend:       c.Int("resend"),
				nc:           c.Int("nc"),
			}

			// init services
			startup(c)
			// init timer
			initTimer(c.Int("rpm-limit"))

			// listeners
			go tcpServer(config)
			go udpServer(config)

			// wait forever
			select {}
		},
	}
	app.Run(os.Args)
}

func tcpServer(config *Config) {
	// resolve address & start listening
	tcpAddr, err := net.ResolveTCPAddr("tcp4", config.listen)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	log.Info("listening on:", listener.Addr())

	// loop accepting
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Warning("accept failed:", err)
			continue
		}
		// set socket read buffer
		conn.SetReadBuffer(config.sockbuf)
		// set socket write buffer
		conn.SetWriteBuffer(config.sockbuf)
		// start a goroutine for every incoming connection for reading
		go handleClient(conn, config)
	}
}

func udpServer(config *Config) {
	l, err := kcp.Listen(config.listen)
	checkError(err)
	log.Info("udp listening on:", l.Addr())
	lis := l.(*kcp.Listener)

	if err := lis.SetReadBuffer(config.sockbuf); err != nil {
		log.Println("SetReadBuffer", err)
	}
	if err := lis.SetWriteBuffer(config.sockbuf); err != nil {
		log.Println("SetWriteBuffer", err)
	}
	if err := lis.SetDSCP(config.dscp); err != nil {
		log.Println("SetDSCP", err)
	}

	// loop accepting
	for {
		conn, err := lis.AcceptKCP()
		if err != nil {
			log.Warning("accept failed:", err)
			continue
		}
		// set kcp parameters
		conn.SetWindowSize(config.sndwnd, config.rcvwnd)
		conn.SetNoDelay(config.nodelay, config.interval, config.resend, config.nc)
		conn.SetStreamMode(true)
		conn.SetMtu(config.mtu)

		// start a goroutine for every incoming connection for reading
		go handleClient(conn, config)
	}
}

// PIPELINE #1: handleClient
// the goroutine is used for reading incoming PACKETS
// each packet is defined as :
// | 2B size |     DATA       |
//
func handleClient(conn net.Conn, config *Config) {
	defer utils.PrintPanicStack()
	defer conn.Close()
	// for reading the 2-Byte header
	header := make([]byte, 2)
	// the input channel for agent()
	in := make(chan []byte)
	defer func() {
		close(in) // session will close
	}()

	// create a new session object for the connection
	// and record it's IP address
	var sess Session
	host, port, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		log.Error("cannot get remote address:", err)
		return
	}
	sess.IP = net.ParseIP(host)
	log.Infof("new connection from:%v port:%v", host, port)

	// session die signal, will be triggered by agent()
	sess.Die = make(chan struct{})

	// create a write buffer
	out := new_buffer(conn, sess.Die, config.txqueuelen)
	go out.start()

	// start agent for PACKET processing
	wg.Add(1)
	go agent(&sess, in, out)

	// read loop
	for {
		// solve dead link problem:
		// physical disconnection without any communcation between client and server
		// will cause the read to block FOREVER, so a timeout is a rescue.
		conn.SetReadDeadline(time.Now().Add(config.readDeadline))

		// read 2B header
		n, err := io.ReadFull(conn, header)
		if err != nil {
			log.Warningf("read header failed, ip:%v reason:%v size:%v", sess.IP, err, n)
			return
		}
		size := binary.BigEndian.Uint16(header)

		// alloc a byte slice of the size defined in the header for reading data
		payload := make([]byte, size)
		n, err = io.ReadFull(conn, payload)
		if err != nil {
			log.Warningf("read payload failed, ip:%v reason:%v size:%v", sess.IP, err, n)
			return
		}

		// deliver the data to the input queue of agent()
		select {
		case in <- payload: // payload queued
		case <-sess.Die:
			log.Warningf("connection closed by logic, flag:%v ip:%v", sess.Flag, sess.IP)
			return
		}
	}
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
}

func startup(c *cli.Context) {
	go sig_handler()
	services.Init(c)
}

var (
	rpmLimit int
)

func initTimer(rpm_limit int) {
	rpmLimit = rpm_limit
}

// 玩家1分钟定时器
func timer_work(sess *Session, out *Buffer) {
	defer func() {
		sess.PacketCount1Min = 0
	}()

	// 发包频率控制，太高的RPS直接踢掉
	if sess.PacketCount1Min > rpmLimit {
		sess.Flag |= SESS_KICKED_OUT
		log.WithFields(log.Fields{
			"userid":  sess.UserId,
			"count1m": sess.PacketCount1Min,
			"total":   sess.PacketCount,
		}).Error("RPM")
		return
	}
}

// PIPELINE #3: buffer
// controls the packet sending for the client
type Buffer struct {
	ctrl    chan struct{} // receive exit signal
	pending chan []byte   // pending packets
	conn    net.Conn      // connection
	cache   []byte        // for combined syscall write
}

// packet sending procedure
func (buf *Buffer) send(sess *Session, data []byte) {
	// in case of empty packet
	if data == nil {
		return
	}

	// encryption
	// (NOT_ENCRYPTED) -> KEYEXCG -> ENCRYPT
	if sess.Flag&SESS_ENCRYPT != 0 { // encryption is enabled
		sess.Encoder.XORKeyStream(data, data)
	} else if sess.Flag&SESS_KEYEXCG != 0 { // key is exchanged, encryption is not yet enabled
		sess.Flag &^= SESS_KEYEXCG
		sess.Flag |= SESS_ENCRYPT
	}

	// queue the data for sending
	select {
	case buf.pending <- data:
	default: // packet will be dropped if txqueuelen exceeds
		log.WithFields(log.Fields{"userid": sess.UserId, "ip": sess.IP}).Warning("pending full")
	}
	return
}

// packet sending goroutine
func (buf *Buffer) start() {
	defer utils.PrintPanicStack()
	for {
		select {
		case data := <-buf.pending:
			buf.raw_send(data)
		case <-buf.ctrl: // receive session end signal
			return
		}
	}
}

// raw packet encapsulation and put it online
func (buf *Buffer) raw_send(data []byte) bool {
	// combine output to reduce syscall.write
	sz := len(data)
	binary.BigEndian.PutUint16(buf.cache, uint16(sz))
	copy(buf.cache[2:], data)

	// write data
	n, err := buf.conn.Write(buf.cache[:sz+2])
	if err != nil {
		log.Warningf("Error send reply data, bytes: %v reason: %v", n, err)
		return false
	}

	return true
}

// create a associated write buffer for a session
func new_buffer(conn net.Conn, ctrl chan struct{}, txqueuelen int) *Buffer {
	buf := Buffer{conn: conn}
	buf.pending = make(chan []byte, txqueuelen)
	buf.ctrl = ctrl
	buf.cache = make([]byte, packet.PACKET_LIMIT+2)
	return &buf
}

var (
	wg sync.WaitGroup
	// server close signal
	die = make(chan struct{})
)

// handle unix signals
func sig_handler() {
	defer utils.PrintPanicStack()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)

	for {
		msg := <-ch
		switch msg {
		case syscall.SIGTERM: // 关闭agent
			close(die)
			log.Info("sigterm received")
			log.Info("waiting for agents close, please wait...")
			wg.Wait()
			log.Info("agent shutdown.")
			os.Exit(0)
		}
	}
}

// PIPELINE #2: agent
// all the packets from handleClient() will be handled
func agent(sess *Session, in chan []byte, out *Buffer) {
	defer wg.Done() // will decrease waitgroup by one, useful for manual server shutdown
	defer utils.PrintPanicStack()

	// init session
	sess.MQ = make(chan pb.Game_Frame, 512)
	sess.ConnectTime = time.Now()
	sess.LastPacketTime = time.Now()
	// minute timer
	min_timer := time.After(time.Minute)

	// cleanup work
	defer func() {
		close(sess.Die)
		if sess.Stream != nil {
			sess.Stream.CloseSend()
		}
	}()

	// >> the main message loop <<
	// handles 4 types of message:
	//  1. from client
	//  2. from game service
	//  3. timer
	//  4. server shutdown signal
	for {
		select {
		case msg, ok := <-in: // packet from network
			if !ok {
				return
			}

			sess.PacketCount++
			sess.PacketCount1Min++
			sess.PacketTime = time.Now()

			if result := route(sess, msg); result != nil {
				out.send(sess, result)
			}
			sess.LastPacketTime = sess.PacketTime
		case frame := <-sess.MQ: // packets from game
			switch frame.Type {
			case pb.Game_Message:
				out.send(sess, frame.Message)
			case pb.Game_Kick:
				sess.Flag |= SESS_KICKED_OUT
			}
		case <-min_timer: // minutes timer
			timer_work(sess, out)
			min_timer = time.After(time.Minute)
		case <-die: // server is shuting down...
			sess.Flag |= SESS_KICKED_OUT
		}

		// see if the player should be kicked out.
		if sess.Flag&SESS_KICKED_OUT != 0 {
			return
		}
	}
}



// route client protocol
func route(sess *Session, p []byte) []byte {
	start := time.Now()
	defer utils.PrintPanicStack(sess, p)
	// 解密
	if sess.Flag&SESS_ENCRYPT != 0 {
		sess.Decoder.XORKeyStream(p, p)
	}
	// 封装为reader
	reader := packet.Reader(p)

	// 读客户端数据包序列号(1,2,3...)
	// 客户端发送的数据包必须包含一个自增的序号，必须严格递增
	// 加密后，可避免重放攻击-REPLAY-ATTACK
	seq_id, err := reader.ReadU32()
	if err != nil {
		log.Error("read client timestamp failed:", err)
		sess.Flag |= SESS_KICKED_OUT
		return nil
	}

	// 数据包序列号验证
	if seq_id != sess.PacketCount {
		log.Errorf("illegal packet sequence id:%v should be:%v size:%v", seq_id, sess.PacketCount, len(p)-6)
		sess.Flag |= SESS_KICKED_OUT
		return nil
	}

	// 读协议号
	b, err := reader.ReadS16()
	if err != nil {
		log.Error("read protocol number failed.")
		sess.Flag |= SESS_KICKED_OUT
		return nil
	}

	// 根据协议号断做服务划分
	// 协议号的划分采用分割协议区间, 用户可以自定义多个区间，用于转发到不同的后端服务
	var ret []byte
	if b > 1000 {
		if err := forward(sess, p[4:]); err != nil {
			log.Errorf("service id:%v execute failed, error:%v", b, err)
			sess.Flag |= SESS_KICKED_OUT
			return nil
		}
	} else {
		if h := client_handler.Handlers[b]; h != nil {
			ret = h(sess, reader)
		} else {
			log.Errorf("service id:%v not bind", b)
			sess.Flag |= SESS_KICKED_OUT
			return nil
		}
	}

	elasped := time.Now().Sub(start)
	if b != 0 { // 排除心跳包日志
		log.WithFields(log.Fields{"cost": elasped,
			"api":  client_handler.RCode[b],
			"code": b}).Debug("REQ")
	}
	return ret
}

var (
	ERROR_STREAM_NOT_OPEN = errors.New("stream not opened yet")
)

// forward messages to game server
func forward(sess *Session, p []byte) error {
	frame := &pb.Game_Frame{
		Type:    pb.Game_Message,
		Message: p,
	}

	// check stream
	if sess.Stream == nil {
		return ERROR_STREAM_NOT_OPEN
	}

	// forward the frame to game
	if err := sess.Stream.Send(frame); err != nil {
		log.Error(err)
		return err
	}
	return nil
}
