package util

import (
	"hash/crc32"
	"io"
	"os"
	"strconv"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"

	"github.com/pingcap/errors"
)

func GetFileSize(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return uint64(fi.Size()), nil
}

func FileExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !fi.IsDir()
}

func DirExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.IsDir()
}

func DeleteFileIfExists(path string) (bool, error) {
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

// CalcCRC32 Calculates the given file's CRC32 checksum.
func CalcCRC32(path string) (uint32, error) {
	digest := crc32.NewIEEE()
	f, err := os.Open(path)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	_, err = io.Copy(digest, f)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return digest.Sum32(), nil
}

func LogEntries(leader int, peer int, entries []pb.Entry) {
	str := ""
	if leader == peer {
		str += "Leader||"
	} else {
		str += "Follower||"
	}
	str += "id:" + strconv.Itoa(peer) + "||"
	str += " "
	if len(entries) < 1 {
		return
	}
	flag := false
	prev := entries[0].Index
	for i, entry := range entries {
		if i > 0 && prev != entry.Index-1 {
			flag = true
		}
		prev = entry.Index
		str += strconv.Itoa(int(entry.Index))
		str += " "
	}
	if flag {
		str += "ERROR!!"
		log.Info(str)
	}
}
