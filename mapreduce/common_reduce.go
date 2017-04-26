package mapreduce

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"log"
	"os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	decoders := make(map[*os.File]*json.Decoder)
	for i := 0; i < nMap; i++ {
		interFile := reduceName(jobName, i, reduceTaskNumber)
		debug("open file: %v\n", interFile)
		file, openErr := os.Open(interFile)
		if openErr != nil {
			log.Fatal(" ")
		}
		inputReader := bufio.NewReader(file)
		dec := json.NewDecoder(inputReader)
		decoders[file] = dec
	}
	// init heap
	h := &SortedFileReaderHeap{}
	heap.Init(h)
	for file, decoder := range decoders {
		reader := SortedFileReader{nil, decoder, file}
		// reader.ReadHead()
		reader.Next()
		heap.Push(h, reader)
	}
	// create output
	output, err := os.Create(outFile)
	if err != nil {
		log.Fatal(" ")
	}
	encoder := json.NewEncoder(output)
	// read values
	var currKey string
	var currValues []string
	for h.HasNext() {
		kv := h.Next()
		if currKey != kv.Key {
			if currKey != "" && currValues != nil && len(currValues) != 0 {
				result := reduceF(currKey, currValues)
				var resKv = KeyValue{currKey, result}
				encoder.Encode(&resKv)
			}
			currKey = kv.Key
			currValues = []string{}
		}
		currValues = append(currValues, kv.Value)
	}
	// be careful! the last value in the file does not been flush!
	if currKey != "" && currValues != nil && len(currValues) != 0 {
		result := reduceF(currKey, currValues)
		var resKv = KeyValue{currKey, result}
		encoder.Encode(&resKv)
	}
}

type SortedFileReader struct {
	CurrKV  *KeyValue
	decoder *json.Decoder
	file    *os.File
}

func (r *SortedFileReader) ReadHead() {
	t, err := r.decoder.Token()
	if err != nil {
		log.Fatal("file %v decode failed", r.file.Name())
	} else {
		log.Print("%T: %v\n", t, t)
	}
}

func (r *SortedFileReader) Close() {
	r.file.Close()
}

func (r *SortedFileReader) HasNext() bool {
	return r.CurrKV != nil || r.decoder.More()
}

func (r *SortedFileReader) Next() *KeyValue {
	var kv KeyValue
	retKv := r.CurrKV
	if r.decoder.More() {
		err := r.decoder.Decode(&kv)
		if err != nil {
			r.CurrKV = nil
			r.file.Close()
		} else {
			r.CurrKV = &kv
		}
		// debug("has more: %v\n", kv.Key)
	} else {
		r.CurrKV = nil
		r.file.Close()
	}
	// debug("file: %v, reader.Next: %v, %v\n", r.file.Name(), kv.Key, kv.Value)
	return retKv
}

type SortedFileReaderHeap []SortedFileReader

func (h SortedFileReaderHeap) Len() int {
	return len(h)
}

func (h SortedFileReaderHeap) Less(i, j int) bool {
	return h[i].CurrKV.Key < h[j].CurrKV.Key
}

func (h SortedFileReaderHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *SortedFileReaderHeap) Push(x interface{}) {
	*h = append(*h, x.(SortedFileReader))
}

func (h *SortedFileReaderHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *SortedFileReaderHeap) Close() {
	// type transmit, Pop(h).(SortedFileReader)
	for h.Len() > 0 {
		reader := heap.Pop(h).(SortedFileReader)
		reader.Close()
	}
}

func (h *SortedFileReaderHeap) HasNext() bool {
	return h.Len() > 0 && (*h)[0].HasNext()
}

func (h *SortedFileReaderHeap) Next() *KeyValue {
	reader := heap.Pop(h).(SortedFileReader)
	kvPtr := reader.Next()

	if reader.HasNext() {
		heap.Push(h, reader)
	}
	return kvPtr
}

func (h *SortedFileReaderHeap) Top() (kv *KeyValue) {
	if h.Len() == 0 {
		kv = nil
	} else {
		topReader := (*h)[0]
		kv = topReader.CurrKV
	}
	// debug("top file: %v, key: %v, value: %v\n", topReader.file.Name(), kv.Key, kv.Value)
	return
}
