package store

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/Diudiuseven/trace-collection-store-go/trace"
)

// wal 负责把上报记录追加到磁盘以支持崩溃恢复。
// 记录格式：4 字节大端长度 + JSON 编码的环节。
// 崩溃可能留下半条尾部记录，回放时读到坏记录即安全停止。
type wal struct {
	mu   sync.Mutex
	f    *os.File
	buf  *bufio.Writer
	path string
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{f: f, buf: bufio.NewWriter(f), path: path}, nil
}

// append 追加一条环节记录并落盘，保证崩溃后仍可恢复。
func (w *wal) append(sp trace.Span) error {
	payload, err := json.Marshal(sp)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := w.buf.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(payload); err != nil {
		return err
	}
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// replay 逐条回放记录。遇到不完整或损坏的尾部记录时停止，
// 已回放的数据不受影响。
func (w *wal) replay(fn func(sp trace.Span) error) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(w.f)
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // 正常结束或半条记录
			}
			return err
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil // 半条记录，视为结束
		}
		var sp trace.Span
		if err := json.Unmarshal(payload, &sp); err != nil {
			return nil // 损坏记录，视为结束
		}
		if err := fn(sp); err != nil {
			return err
		}
	}
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}
