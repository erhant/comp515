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

// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

func check(e error) {
	if e != nil {
		fmt.Println(e)
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

func recieveStringMessageFromServer(conn net.Conn) string {
	messagercv, err := bufio.NewReader(conn).ReadString('\n')
	check(err)
	messagercv = strings.TrimSpace(messagercv)
	return messagercv
}
func main() {
	// Command line arguments
	argsWithoutProg := os.Args[1:]
	var serverPort, serverIP, ipandport string
	if len(os.Args) == 1 {
		serverIP = "localhost"
		serverPort = "8080"
	} else {
		serverIP = argsWithoutProg[0]
		serverPort = argsWithoutProg[1]
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

	loggedIn := false
	clientUsername := ""

	scanner := bufio.NewScanner(os.Stdin)

	for {
		if loggedIn {
			fmt.Print("\t1) Change user (" + clientUsername + ")\n\t2) Enter the filename to store\n\t3) Enter the filename to retrieve\n\t4) Exit\n>Please select an option: ")
		} else {
			fmt.Print("\t1) Enter the username\n\t2) Enter the filename to store\n\t3) Enter the filename to retrieve\n\t4) Exit\n>Please select an option: ")
		}

		scanner.Scan()
		input := scanner.Text()
		switch input {
		case "1":
			if loggedIn {
				fmt.Print(">Enter new username: ")
				scanner.Scan()
				username := scanner.Text()
				if username == clientUsername {
					fmt.Println("This user is already logged in at this client.")
				} else {
					// Send 1,username,\n
					writeToServer(conn, []byte("1"+username+"\n"))
					// Expect a messagercv
					fmt.Println("\tServer >>>\n" + recieveStringMessageFromServer(conn) + "\n\t n")
					clientUsername = username
					loggedIn = true
				}
			} else {
				fmt.Print(">Enter the username: ")
				scanner.Scan()
				username := scanner.Text()
				// Send 1,username,\n
				writeToServer(conn, []byte("1"+username+"\n"))
				// Expect a messagercv
				fmt.Println("\tServer >>>\n" + recieveStringMessageFromServer(conn) + "\n\t>>>\n")
				clientUsername = username
				loggedIn = true
			}
		case "2":
			if loggedIn {
				fmt.Print(">Enter the filename to send: ")
				scanner.Scan()
				filename := scanner.Text()
				filename = strings.TrimSpace(filename)
				// Check if file exists
				if fileExists(filename) {
					dat, err := ioutil.ReadFile(filename)
					check(err)
					datLen := len(dat)
					// Send 2,username,\n,filename,\n,filebytecount,\n,filebytes
					bytesToSend := append([]byte("2"+clientUsername+"\n"+filename+"\n"), []byte(strconv.Itoa(datLen) + "\n")[:]...)
					bytesToSend = append(bytesToSend, dat[:]...)
					//// START WATCH
					starttime := time.Now()
					writeToServer(conn, bytesToSend)
					// Expect a messagercv
					fmt.Println("\tServer >>>\n" + recieveStringMessageFromServer(conn) + "\n\t>>>\n")
					/// STOP WATCH
					elapsed := time.Since(starttime)
					log.Printf("FILE STORE time took %s\n", elapsed)
				} else {
					fmt.Println("No such file.")
				}

			} else {
				fmt.Println("Please login first.")
			}
		case "3":
			if loggedIn {
				reader := bufio.NewReader(conn)
				fmt.Print(">Enter the filename to recieve: ")
				scanner.Scan()
				filename := scanner.Text()
				filename = strings.TrimSpace(filename)
				//// START WATCH
				starttime := time.Now()
				// Send 3,username,\n,filename,\n
				writeToServer(conn, []byte("3"+clientUsername+"\n"+filename+"\n"))
				// Expect responseByte,filebytecount,\n,filebyte
				messageTypeByte, _ := reader.ReadByte()
				switch messageTypeByte {
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
				case []byte("2")[0]:
					fmt.Println("\tServer >>>\n" + recieveStringMessageFromServer(conn) + "\n\t>>>\n")
				}
			} else {
				fmt.Println("Please login first.")
			}
		case "4":
			if loggedIn {
				// send 4
				writeToServer(conn, []byte("4"))
				// expect message
				fmt.Println("\tServer >>>\n" + recieveStringMessageFromServer(conn) + "\n\t>>>\n")
			} else {
				fmt.Println("Exiting...")
			}
			return
		default:
			fmt.Println("Invalid option.")
		}

	}

}
