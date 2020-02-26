package reusepipe

import (
	"encoding/binary"
	"fmt"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

const (
	PipeGetID uint8 = 1
	PipeRetID uint8 = 2
	PipeData  uint8 = 3
	PipeAck   uint8 = 4
	PipeClose uint8 = 5
)

type Pipe struct {
	net.Conn
	id          uint32
	pipes       *Pipes
	buf         chan []byte
	tmpBuf      []byte
	closed      bool
	remoteClose bool
	*sync.Mutex
}

type Pipes struct {
	conn   net.Conn
	pipes  map[uint32]*Pipe
	nextID uint32
	retID  chan uint32
	getID  chan uint32
	closed bool
	*sync.Mutex
}

func NewPipes(conn net.Conn) *Pipes {
	var pipes = &Pipes{
		conn:   conn,
		pipes:  make(map[uint32]*Pipe),
		retID:  make(chan uint32, 1024),
		getID:  make(chan uint32, 1024),
		nextID: 22,
		Mutex:  &sync.Mutex{},
	}
	go pipes.ReadLoop()
	go pipes.Update()
	return pipes
}

func (p *Pipes) NewPipe() (net.Conn, error) {
	var b = []byte{PipeGetID}
	p.Lock()
	n, err := p.conn.Write(b)
	p.Unlock()
	if n <= 0 {
		return nil, err
	}
	var id uint32
	select {
	case id = <-p.retID:
		if id == 0 {
			return nil, errClose
		}
	case <-time.After(10 * time.Second):
		return nil, errClose
	}

	p.Lock()
	pp := &Pipe{
		Conn:  p.conn,
		pipes: p,
		id:    id,
		buf:   make(chan []uint8, 256),
		Mutex: &sync.Mutex{},
	}
	p.pipes[id] = pp
	p.Unlock()
	return pp, nil
}

func (p *Pipes) Accept() (net.Conn, error) {
	id := <-p.getID
	if id == 0 {
		return nil, errClose
	}
	p.Lock()
	pp := &Pipe{
		Conn:  p.conn,
		pipes: p,
		id:    id,
		buf:   make(chan []uint8, 256),
		Mutex: &sync.Mutex{},
	}
	p.pipes[id] = pp
	p.Unlock()
	return pp, nil
}

func (p *Pipes) Update() {
	for {
		var b = []byte{PipeAck}
		p.Lock()
		n, err := p.conn.Write(b)
		p.Unlock()
		if n <= 0 || err != nil {
			p.closed = true
			break
		}
		time.Sleep(time.Second * 10)
	}
}

func (p *Pipes) ReadLoop() {
	defer func() {
		p.closed = true
		close(p.getID) // 生产者负责close，所以在这里关
		close(p.retID) // 生产者负责close，所以在这里关

		p.Lock()
		for _, pipe := range p.pipes {
			// 不要调用pipe.close()
			pipe.remoteClose = true // 不要发了，conn都关了
			pipe.closed = true      // 关闭写
			close(pipe.buf)         // 关闭读（仍然可以读）
		}
		p.Unlock()

		p.conn.Close() // 关闭，但不置为空指针，是个多线程好习惯。

		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()
	var buf [4096 + 7]uint8
	remain := 0
	for {
		n, _ := p.conn.Read(buf[remain:])
		if n <= 0 {
			break
		}
		n += remain

		readed := 0
		needMore := false
		for readed < n {
			switch buf[readed] {
			case PipeData:
				needMore = true
				if len(buf[readed:n]) > 7 {
					id := binary.BigEndian.Uint32(buf[readed+1:])
					nlen := int(binary.BigEndian.Uint16(buf[readed+5:]))
					if nlen > 4096 {
						panic("nlen > 4096")
					}
					if len(buf[readed:n])-7 >= nlen && nlen > 0 {
						p.Lock()
						c, ok := p.pipes[id]
						p.Unlock()
						if ok {
							bb := make([]byte, nlen)
							copy(bb, buf[readed+7:readed+7+nlen])
							c.buf <- bb
						}
						readed += 1 + 4 + 2 + nlen
						needMore = false
					}
				}
			case PipeGetID:
				var b [5]byte
				b[0] = PipeRetID
				binary.BigEndian.PutUint32(b[1:], p.nextID)
				p.Lock()
				p.conn.Write(b[:])
				p.Unlock()
				p.getID <- p.nextID
				p.nextID++
				if p.nextID == 0 {
					p.nextID++
				}
				readed++
			case PipeRetID:
				needMore = true
				if len(buf[readed:n]) >= 5 {
					id := binary.BigEndian.Uint32(buf[readed+1:])
					p.retID <- id
					readed += 5
					needMore = false
				}
			case PipeAck:
				readed++
			case PipeClose:
				needMore = true
				if len(buf[readed:n]) >= 5 {
					id := binary.BigEndian.Uint32(buf[readed+1:])
					p.Lock()
					c, ok := p.pipes[id]
					p.Unlock()
					if ok {
						c.remoteClose = true
						c.Close()
					}
					readed += 5
					needMore = false
				}
			default:
				panic(fmt.Sprint(readed, n, buf[readed]))
			}
			if needMore {
				break
			}

		}

		remain = n - readed
		if readed < n {
			copy(buf[:], buf[readed:n])
		}
	}
}

type PipeError struct {
	error
	timeout bool
	closed  bool
	msg     string
}

func (e PipeError) Timeout() bool {
	return e.timeout
}

func (e PipeError) Temporary() bool {
	return !e.closed
}

func (e PipeError) Error() string {
	return e.msg
}

var errTimeout = PipeError{
	timeout: true,
	msg:     "pipe time out",
}

var errClose = PipeError{
	closed: true,
	msg:    "pipe closed",
}

func (p *Pipe) Write(b []byte) (n int, err error) {
	if p.closed || p.pipes.closed {
		return 0, errClose
	}

	var bb [4096 + 7]byte
	for i := 0; i < len(b); i += 4096 {
		bb[0] = PipeData
		binary.BigEndian.PutUint32(bb[1:], p.id)
		var slen = len(b[i:])
		if slen > 4096 {
			slen = 4096
		}
		binary.BigEndian.PutUint16(bb[5:], uint16(slen))
		copy(bb[7:], b[i:])
		p.pipes.Lock()
		n, err = p.Conn.Write(bb[:slen+7])
		p.pipes.Unlock()
		if n <= 0 {
			return n, err
		}
	}
	return len(b), err
}

func (p *Pipe) Read(b []byte) (n int, err error) {

	// 这里不判断, pipe close之后还可以读数据
	// if p.pipes.closed {
	// 	return 0, errClose
	// }

	var buf []byte
	if len(p.tmpBuf) > 0 {
		buf = p.tmpBuf
	} else {
		select {
		case buf = <-p.buf:
			if len(buf) <= 0 {
				return 0, errClose
			}
		case <-time.After(600 * time.Second):
			return 0, errTimeout
		}
	}

	copy(b, buf)
	var l = len(buf)
	if len(b) < l {
		l = len(b)
		p.tmpBuf = buf[len(b):]
	} else {
		p.tmpBuf = p.tmpBuf[:0]
	}
	return l, nil
}

func (p *Pipe) Close() error {
	p.pipes.Lock()
	delete(p.pipes.pipes, p.id)
	if p.closed == false {
		close(p.buf) // 这里也可以close，因为从map中删除了，而p.closed==false
	}
	p.closed = true // 让pipe不要写了
	if p.remoteClose == false {
		// 让对面不要写过来了
		var b [5]byte
		b[0] = PipeClose
		binary.BigEndian.PutUint32(b[1:], p.id)
		p.Conn.Write(b[:])
		p.remoteClose = true
	}
	p.pipes.Unlock()
	return nil
}

func (p *Pipe) SetReadDeadline(t time.Time) error {
	// TODO: 加一个读超时有必要
	return nil
}

func (p *Pipe) SetWriteDeadline(t time.Time) error {
	return nil
}

func (p *Pipe) SetDeadline(t time.Time) error {
	return nil
}
