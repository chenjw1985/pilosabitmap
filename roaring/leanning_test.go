package roaring

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"unsafe"
)

func TestBitmap(t *testing.T) {
	const (
		containerNil    byte = iota // no container
		containerArray              // slice of bit position values
		containerBitmap             // slice of 1024 uint64s
		containerRun                // container of run-encoded bits
	)
	headerBaseSize := int64(3 + 1 + 4)
	runCountHeaderSize := uint64(2)

	a := NewBitmap(1, 2, 4, 8, 16, 128, 256, 1024, 65535, 65536)
	buf := new(bytes.Buffer)
	a.WriteTo(buf)
	data := buf.Bytes()
	t.Log("Roaring Size:", len(data))
	// MagicNumber
	t.Log("MagicNumber:", binary.LittleEndian.Uint16(data[0:2]))
	// storageVersion
	t.Log("storageVersion:", uint32(data[2]))
	// key count
	keys := int64(binary.LittleEndian.Uint32(data[4:8]))
	t.Log("key count:", keys)
	headerStart := headerBaseSize
	t.Log("headerStart:", headerStart)
	headerEnd := headerStart + (keys * 12)
	t.Log("headerEnd:", headerEnd)
	offsetStart := headerEnd
	t.Log("offsetStart:", offsetStart)
	// offsetEnd = headerBaseSize+keys*16
	offsetEnd := offsetStart + (keys * 4)
	t.Log("offsetEnd:", offsetEnd)
	currentDataOffset := offsetEnd
	t.Log("currentDataOffset:", currentDataOffset)

	t.Log("data[0]:", data[0])

	r := new(pilosaRoaringIterator)
	r.data = data
	r.headers = data[headerStart:headerEnd]
	r.offsets = data[offsetStart:offsetEnd]
	r.currentIdx = 0
	header := r.headers[r.currentIdx*12:]
	r.currentKey = binary.LittleEndian.Uint64(header[0:8])
	t.Log("r.currentKey:", r.currentKey)
	r.currentType = byte(binary.LittleEndian.Uint16(header[8:10]))
	t.Log("r.currentType:", r.currentType)
	r.currentN = int(binary.LittleEndian.Uint16(header[10:12])) + 1
	t.Log("r.currentN:", r.currentN)
	r.currentDataOffset = binary.LittleEndian.Uint64(r.offsets[r.currentIdx*4:])
	t.Log("r.currentDataOffset:", r.currentDataOffset)
	// a run container keeps its data after an initial 2 byte length header
	var runCount uint16
	if r.currentType == containerRun {
		runCount = binary.LittleEndian.Uint16(data[r.currentDataOffset : r.currentDataOffset+runCountHeaderSize])
		r.currentDataOffset += 2
	}
	t.Log("runCount:", runCount)
	r.currentPointer = (*uint16)(unsafe.Pointer(&data[r.currentDataOffset]))
	t.Log("r.currentPointer:", r.currentPointer)
	var size int
	switch r.currentType {
	case containerArray:
		r.currentLen = r.currentN
		size = r.currentLen * 2
	case containerBitmap:
		r.currentLen = 1024
		size = 8192
	case containerRun:
		r.currentLen = int(runCount)
		size = r.currentLen * 4
	}
	r.currentDataOffset += uint64(size)
	r.lastErr = nil
	t.Log("r.currentLen:", r.currentLen)
	t.Log("r.currentDataOffset:", r.currentDataOffset)

	v := uint64(512)
	var cont *Container
	hb := highbits(v)
	t.Log("hb:", hb)
	cont = a.Containers.GetOrCreate(hb)
	t.Log("cont:", cont)
	newC, change := (*Container).add(cont, lowbits(v))
	t.Log("change:", change)
	t.Log("newC:", newC)
	if newC != cont {
		a.Containers.Put(hb, newC)
		cont = newC
	}
	getHighbitsLowbits(v)
	//*(*[]uint16)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(c.pointer)), Len: int(c.len), Cap: int(c.cap)}))

	t.Log("a.Count():", a.Count())
	t.Log("a.Slice():", a.Slice())
}

func getHighbitsLowbits(x uint64) {
	fmt.Println("highbits:", x>>16, ",lowbits:", uint16(x&0xFFFF))
}

func TestHighbitsLowbits(t *testing.T) {
	getHighbitsLowbits(2)
	getHighbitsLowbits(256)
	getHighbitsLowbits(1024)
	getHighbitsLowbits(65535)
	getHighbitsLowbits(65536)
	getHighbitsLowbits(65536 * 10000)
	getHighbitsLowbits(65535 * 65535 * 65535)
}

func createBitmap() string {
	name := "123.bm"
	a := NewBitmap(1, 2, 4, 8, 16, 128, 256, 1024, 65535, 65536)
	buf := new(bytes.Buffer)
	a.WriteTo(buf)
	ioutil.WriteFile(name, buf.Bytes(), 0750)
	fmt.Println("buf:", buf.Bytes())
	return name
}

func TestBitmapFile(t *testing.T) {
	const (
		containerNil    byte = iota // no container
		containerArray              // slice of bit position values
		containerBitmap             // slice of 1024 uint64s
		containerRun                // container of run-encoded bits
	)
	const headerBaseSize = int64(3 + 1 + 4)
	const runCountHeaderSize = 2

	data, _ := ioutil.ReadFile(createBitmap())
	t.Log("Roaring Size:", len(data))
	t.Log("MagicNumber:", binary.LittleEndian.Uint16(data[0:2]))
	keys := int64(binary.LittleEndian.Uint32(data[4:8]))
	t.Log("key count:", keys)

	kb1 := data[headerBaseSize : headerBaseSize+8]
	ki1 := binary.LittleEndian.Uint64(kb1)
	t.Log("kb1", kb1, ki1)
	data1 := data[40:58]
	for i, l := 0, len(data1); i < l; i += 2 {
		t.Log(i, l, data1[i:i+2], uint64(binary.LittleEndian.Uint16(data1[i:i+2]))+ki1*65536)
	}

	kb2 := data[headerBaseSize+12 : headerBaseSize+12+8]
	ki2 := binary.LittleEndian.Uint64(kb2)
	t.Log("kb2", kb2, ki2)
	data2 := data[58:60]
	t.Log(data2, uint64(binary.LittleEndian.Uint16(data2))+ki2*65536)

	a := NewBitmap()
	a.UnmarshalBinary(data)

}

func TestBinarySearch(t *testing.T) {
	value := uint64(2)
	a := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}
	n := len(a)
	lo, hi := 0, n-1
	for lo+16 <= hi {
		i := int(uint((lo + hi)) >> 1)
		v := a[i]

		if v < value {
			lo = i + 1
		} else if v > value {
			hi = i - 1
		} else {
			t.Log(value, "'s idx:", i)
			return
		}
	}
	t.Log("lo:", lo, ",hi:", hi)
	for ; lo <= hi; lo++ {
		v := a[lo]
		if v == value {
			t.Log(value, "'s idx:", lo)
			return
		} else if v > value {
			break
		}
	}
	t.Log(value, "'s idx:", -(lo + 1))
}

func Test0XFFFFFFF(t *testing.T) {
	fmt.Println(0xFFFFFFF)
	data := [10]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	arr := (*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[0]))[:10:10]
	fmt.Println(arr)
	h := (*reflect.SliceHeader)(unsafe.Pointer(&arr))
	fmt.Println(h)

	a2 := make([]uint16, len(arr))
	copy(a2, arr)
	h = (*reflect.SliceHeader)(unsafe.Pointer(&a2))
	fmt.Println(h)
}

func BenchmarkSliceCopy(b *testing.B) {
	data, _ := ioutil.ReadFile(createBitmap())
	for i := 0; i < b.N; i++ {
		sliceCopy(data)
	}
}

func TestSliceCopy(t *testing.T) {
	data, _ := ioutil.ReadFile(createBitmap())
	sliceCopy(data)
}

func sliceCopy(data []byte) {
	array := (*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[40]))[:9:9]
	fmt.Println("array:", array)
	c := new(Container)
	// no array: start with our default 5-value array
	if array == nil {
		c.pointer, c.len, c.cap = (*uint16)(unsafe.Pointer(&c.data[0])), 0, stashedArraySize
		c.n = c.len
		return
	}
	h := (*reflect.SliceHeader)(unsafe.Pointer(&array))
	if h.Data == uintptr(unsafe.Pointer(c.pointer)) {
		// nothing to do but update length
		c.len = int32(h.Len)
		c.n = c.len
		return
	}
	// array we can fit in data store:
	if len(array) <= stashedArraySize {
		copy(c.data[:stashedArraySize], array)
		c.pointer, c.len, c.cap = (*uint16)(unsafe.Pointer(&c.data[0])), int32(len(array)), stashedArraySize
		c.n = c.len
		c.flags &^= flagMapped // this is no longer using a hypothetical mmapped input array
		return
	}
	// should make c.pointer point to
	c.pointer, c.len, c.cap = (*uint16)(unsafe.Pointer(h.Data)), int32(h.Len), int32(h.Cap)
	c.n = c.len

	carray := c.array()
	//carray:=*(*[]uint16)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(c.pointer)), Len: int(c.len), Cap: int(c.cap)}))
	fmt.Println("carray:", carray)
	cbytes := (*[0xFFFFFFF]byte)(unsafe.Pointer(&carray[0]))[: 2*c.N() : 2*c.N()]
	fmt.Println("cbytes:", cbytes)
}

func TestKeepAlive(t *testing.T) {
	// A very simplified example showing where KeepAlive is required:
	type File struct{ d int }
	d, err := syscall.Open("./123.bm", syscall.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	p := &File{d}
	runtime.SetFinalizer(p, func(p *File) { syscall.Close(p.d) })
	var buf [10]byte
	n, err := syscall.Read(p.d, buf[:])
	fmt.Println(n, err)
	// Ensure p is not finalized until Read returns.
	runtime.KeepAlive(p)
	// No more uses of p after this point.
}

func TestUnsafe1(t *testing.T) {
	arr := [10]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	size := len(arr)
	p := uintptr(unsafe.Pointer(&arr))

	var data []byte

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Data = p
	sh.Len = size
	sh.Cap = size

	arr[0] = 10

	fmt.Println(data, arr)

	runtime.KeepAlive(arr)
}

func TestUnsafe2(t *testing.T) {
	arr := [10]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	size := len(arr)
	p := uintptr(unsafe.Pointer(&arr))

	sh := &reflect.SliceHeader{
		Data: p,
		Len:  size,
		Cap:  size,
	}

	data := *(*[]byte)(unsafe.Pointer(sh))
	arr[0] = 10
	fmt.Println(data, arr)

	runtime.KeepAlive(arr)
}

func TestArray(t *testing.T) {
	arr := [10]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	t.Log(arr[:5:5])
}
