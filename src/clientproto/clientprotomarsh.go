package clientproto

import (
	"io"
	"sync"
)

func (t *Ping) BinarySize() (nbytes int, sizeKnown bool) {
	return 12, true
}

type PingCache struct {
	mu	sync.Mutex
	cache	[]*Ping
}

func NewPingCache() *PingCache {
	c := &PingCache{}
	c.cache = make([]*Ping, 0)
	return c
}

func (p *PingCache) Get() *Ping {
	var t *Ping
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Ping{}
	}
	return t
}
func (p *PingCache) Put(t *Ping) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Ping) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp64 := t.Ts
	bs[4] = byte(tmp64)
	bs[5] = byte(tmp64 >> 8)
	bs[6] = byte(tmp64 >> 16)
	bs[7] = byte(tmp64 >> 24)
	bs[8] = byte(tmp64 >> 32)
	bs[9] = byte(tmp64 >> 40)
	bs[10] = byte(tmp64 >> 48)
	bs[11] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *Ping) Unmarshal(wire io.Reader) error {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ts = uint64((uint64(bs[4]) | (uint64(bs[5]) << 8) | (uint64(bs[6]) << 16) | (uint64(bs[7]) << 24) | (uint64(bs[8]) << 32) | (uint64(bs[9]) << 40) | (uint64(bs[10]) << 48) | (uint64(bs[11]) << 56)))
	return nil
}

func (t *PingReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type PingReplyCache struct {
	mu	sync.Mutex
	cache	[]*PingReply
}

func NewPingReplyCache() *PingReplyCache {
	c := &PingReplyCache{}
	c.cache = make([]*PingReply, 0)
	return c
}

func (p *PingReplyCache) Get() *PingReply {
	var t *PingReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PingReply{}
	}
	return t
}
func (p *PingReplyCache) Put(t *PingReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PingReply) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.ShardId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32_1 := t.ReplicaId
	bs[4] = byte(tmp32_1)
	bs[5] = byte(tmp32_1 >> 8)
	bs[6] = byte(tmp32_1 >> 16)
	bs[7] = byte(tmp32_1 >> 24)
	tmp64 := t.Ts
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *PingReply) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.ShardId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ReplicaId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ts = uint64((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}
