package protocol

// crc16Table 为 CRC-16/CCITT-FALSE 的查表（多项式 0x1021，初值 0xFFFF，不反射）。
// 覆盖 Header 前 12 字节，防止 Length 字段在传输中损坏导致按错误长度分配内存。
var crc16Table = func() [256]uint16 {
	var t [256]uint16
	for i := 0; i < 256; i++ {
		c := uint16(i) << 8
		for j := 0; j < 8; j++ {
			if c&0x8000 != 0 {
				c = (c << 1) ^ 0x1021
			} else {
				c <<= 1
			}
		}
		t[i] = c
	}
	return t
}()

// CRC16 计算 CRC-16/CCITT-FALSE。
func CRC16(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc = (crc << 8) ^ crc16Table[byte(crc>>8)^b]
	}
	return crc
}
