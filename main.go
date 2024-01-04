package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"unsafe"
)

var StationDelimeter = []byte(";")[0]
var MeasurementDelimeter = []byte("\n")[0]

type Measurements struct {
	Min   float32
	Max   float32
	Sum   float32
	Count int
}

type Range struct {
	Begin int64 // Inclusive
	End   int64 // Exclusive
}

type AggregationResult struct {
	Measurements map[string]*Measurements
	Error        error
}

func determineRanges(filename string, n int64, delim byte) ([]Range, error) {
	var ranges []Range
	var b = make([]byte, 1)

	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return []Range{}, err
	}
	length, err := f.Seek(0, 2)
	if err != nil {
		return []Range{}, err
	}
	defer f.Close()

	if n == 1 {
		r := Range{Begin: 0, End: length}
		ranges = append(ranges, r)
		return ranges, nil
	}

	approxSize := length / n

	if approxSize == 0 {
		return []Range{}, fmt.Errorf("Num ranges (%d) too large for file length (%d)", n, length)
	}

	// First
	first := Range{Begin: 0, End: approxSize}
	ranges = append(ranges, first)

	// Adjust end
	for {
		f.Seek(ranges[0].End, 0)
		f.Read(b)
		if b[0] == delim {
			// We want to be one past the delimiter for an exclusive range on the right
			ranges[0].End++
			break
		}
		ranges[0].End--
	}

	// We could also check that End > Begin, but this covers that case for the first range
	// For upcoming range we could also check the above (but not < 0), but if we catch this for the first range
	// then we should be good to go (?)
	if ranges[len(ranges)-1].End < 0 {
		return []Range{}, fmt.Errorf("Invalid range end %d (expected greater than 0). Too many chunks for file size?", ranges[len(ranges)-1].End)
	}

	// Middle
	for i := int64(1); i < n-1; i++ {
		previous := ranges[i-1]
		begin := previous.End
		end := begin + approxSize
		middle := Range{Begin: begin, End: end}

		for {
			f.Seek(middle.End, 0)
			f.Read(b)
			if b[0] == delim {
				// We want to be one past the delimiter for an exclusive range on the right
				middle.End++
				break
			}
			middle.End--
		}

		ranges = append(ranges, middle)
	}

	// Last
	last := Range{Begin: ranges[len(ranges)-1].End, End: length}
	ranges = append(ranges, last)

	return ranges, nil
}

func printRanges(filename string, ranges []Range) error {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, r := range ranges {
		fmt.Printf("Range: %d-%d", r.Begin, r.End)
		b := make([]byte, r.End-r.Begin)
		f.Seek(r.Begin, 0)
		f.Read(b)
		fmt.Println(string(b))
		fmt.Println("-----")
	}

	return nil
}

func writeOutput(stations *[]string, readings *map[string]*Measurements) (string, error) {
	numStations := len(*stations)

	buffer := new(bytes.Buffer)
	buffer.WriteString("{")
	for i, station := range *stations {
		v, ok := (*readings)[station]
		mean := v.Sum / float32(v.Count)
		if !ok {
			return "", fmt.Errorf("Missing entry for key '%s' (expected this to exist as we've seen this key before)", station)
		}
		buffer.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, v.Min, mean, v.Max))
		if i < numStations-1 {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString("}")

	output := buffer.String()
	return output, nil
}

func task(filename string, r Range, aggregationResultChannel chan AggregationResult) {
	readings := make(map[string]*Measurements)

	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		result := AggregationResult{Error: err}
		aggregationResultChannel <- result
	}
	defer f.Close()

	sectionReader := io.NewSectionReader(f, r.Begin, r.End-r.Begin)
	scanner := bufio.NewScanner(sectionReader)

	for scanner.Scan() {
		buffer := scanner.Bytes()

		stationDelimeterPos := 0
		for {
			if buffer[stationDelimeterPos] == StationDelimeter {
				break
			}
			stationDelimeterPos++
		}

		station := string(buffer[:stationDelimeterPos])

		measurementStr := unsafe.String(unsafe.SliceData(buffer[stationDelimeterPos+1:]), len(buffer)-stationDelimeterPos-1)
		measurement, err := strconv.ParseFloat(measurementStr, 32)
		if err != nil {
			result := AggregationResult{Error: err}
			aggregationResultChannel <- result
		}

		m32 := float32(measurement)

		v, ok := readings[station]
		if ok {
			v.Sum = v.Sum + m32
			v.Min = min(v.Min, m32)
			v.Max = max(v.Max, m32)
			v.Count = v.Count + 1
		} else {
			new := Measurements{
				Sum:   m32,
				Min:   m32,
				Max:   m32,
				Count: 1,
			}
			readings[station] = &new
		}

	}

	result := AggregationResult{
		Measurements: readings,
	}
	aggregationResultChannel <- result
}

func run(filename string, parallel int64) (string, error) {
	ranges, err := determineRanges(filename, parallel, MeasurementDelimeter)
	if err != nil {
		return "", err
	}
	// printRanges(filename, ranges)

	aggregatorChannel := make(chan AggregationResult, parallel)

	var wg sync.WaitGroup

	for i := int64(0); i < parallel; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			task(filename, ranges[i], aggregatorChannel)
		}()
	}

	// Aggregate results
	readings := make(map[string]*Measurements)
	for i := int64(0); i < parallel; i++ {
		subreadings := <-aggregatorChannel
		if subreadings.Error != nil {
			return "", subreadings.Error
		}
		for station, measurements := range subreadings.Measurements {
			v, ok := readings[station]
			if ok {
				v.Sum = v.Sum + measurements.Sum
				v.Min = min(v.Min, measurements.Min)
				v.Max = max(v.Max, measurements.Max)
				v.Count = v.Count + measurements.Count
			} else {
				new := Measurements{
					Sum:   measurements.Sum,
					Min:   measurements.Min,
					Max:   measurements.Max,
					Count: measurements.Count,
				}
				readings[station] = &new
			}
		}
	}

	wg.Wait()
	close(aggregatorChannel)

	var stations []string
	for k := range readings {
		stations = append(stations, k)
	}

	sort.Strings(stations)

	output, err := writeOutput(&stations, &readings)
	if err != nil {
		return "", err
	}

	return output, nil
}

func main() {
	parallel := flag.Int64("parallel", 1, "")
	profileCPUFile := flag.String("profile-cpu", "", "Profile CPU and write to `file`")

	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Printf("Usage: %s <MEASUREMENTS>\n", os.Args[0])
		os.Exit(1)
	}

	if *profileCPUFile != "" {
		f, err := os.Create(*profileCPUFile)
		if err != nil {
			log.Fatal("Could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("Could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	measurementsFile := flag.Arg(0)
	output, err := run(measurementsFile, *parallel)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(output)
}
