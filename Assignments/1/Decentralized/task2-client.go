package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func check(e error) {
	if e != nil {
		log.Fatal(e)
		panic(e)
	}
}

// fileExists checks if a file exists and is not a directory before we try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func writeToServer(conn net.Conn, bytes []byte) {
	conn.Write(bytes)
}

func recieveStringMessageFromServer(conn net.Conn) {
	messagercv, err := bufio.NewReader(conn).ReadString('\n')
	check(err)
	messagercv = strings.TrimSpace(messagercv)
	fmt.Println("\tServer >>>\n" + messagercv + "\n\t>>>\n")
}

func main() {
	// Command line arguments
	argsWithoutProg := os.Args[1:]
	var serverPort, serverIP, ipandport string
	if len(os.Args) == 1 {
		serverIP = "localhost"
		serverPort = "8080"
	} else {
		serverIP = argsWithoutProg[0]   // peer IP
		serverPort = argsWithoutProg[1] // peer Port
	}

	ipandport = serverIP + ":" + serverPort
	fmt.Println("Dialing " + ipandport)
	conn, err := net.Dial("tcp", ipandport)

	defer conn.Close() // defer connection close just in case

	if err != nil {
		// handle error
		fmt.Println(err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)

	for {

		fmt.Print("\t1) Enter the filename to store\n\t2) Enter the filename to retrieve\n\t3) Exit\n>Please select an option: ")
		scanner.Scan()
		input := scanner.Text()

		switch input {
		case "1":
			fmt.Print(">Enter the filename to send: ")
			scanner.Scan()
			filename := scanner.Text()
			filename = strings.TrimSpace(filename)
			// Check if file exists
			if fileExists(filename) {
				dat, err := ioutil.ReadFile(filename)
				check(err)
				datLen := len(dat)
				// Send 1,\n,filename,\n,filebytecount,\n,filebytes
				bytesToSend := append([]byte("1"+filename+"\n"), []byte(strconv.Itoa(datLen) + "\n")[:]...)
				bytesToSend = append(bytesToSend, dat[:]...)
				//// START WATCH
				starttime := time.Now()
				writeToServer(conn, bytesToSend)
				// Expect a messagercv
				recieveStringMessageFromServer(conn)
				/// STOP WATCH
				elapsed := time.Since(starttime)
				log.Printf("FILE STORE time took %s\n", elapsed)
			} else {
				fmt.Println("No such file.")
			}
		case "2":
			reader := bufio.NewReader(conn)
			fmt.Print(">Enter the filename to recieve: ")
			scanner.Scan()
			filename := scanner.Text()
			filename = strings.TrimSpace(filename)
			//// START WATCH
			starttime := time.Now()
			// Send 2,\n,filename,\n
			writeToServer(conn, []byte("2"+filename+"\n"))
			// Expect responseByte,filebytecount,\n,filebyte
			messageTypeByte, _ := reader.ReadByte()
			switch messageTypeByte {
			// if responseByte is 1 file is here
			case []byte("1")[0]:
				byteCountStr, err := reader.ReadString('\n')
				check(err)
				byteCountStr = strings.TrimSpace(byteCountStr)
				byteCount, err := strconv.Atoi(byteCountStr)
				check(err)
				// Read bytes
				var bytes []byte
				for i := 0; i < byteCount; i++ {
					b, err := reader.ReadByte()
					check(err)
					bytes = append(bytes, b)
				}
				/// STOP WATCH
				elapsed := time.Since(starttime)
				log.Printf("FILE RETRIEVAL time took %s\n", elapsed)
				// Write file to the local storage
				err = ioutil.WriteFile(filename, bytes, 0644)
				check(err)
				fmt.Println("File " + filename + " (" + byteCountStr + "B) has been retrieved from server.")
			// if responseByte is 2 file does not exist anywhere
			case []byte("2")[0]:
				recieveStringMessageFromServer(conn)
			}
		case "3":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid option.")
		}

	}

}
