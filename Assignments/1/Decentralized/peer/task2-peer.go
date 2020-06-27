package main

// tmux diye bir sey var, serverdaki terminali kapadiginda programin kapanmamasini sagliyor. NodeJS'teki pm2 gibi bir sey yani.

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
)

// Peer is the struct for a peer in the logical ring. It has id, ip, port and its own predecesseor and successor peers.
type Peer struct {
	id       uint32
	ip       string
	port     string
	predID   uint32
	predIP   string
	predPort string
	succID   uint32
	succIP   string
	succPort string
}

var thisPeer Peer // the peer struct for this note.
var modulo uint32 // global modulo variable

// Hash a string
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32() % modulo
}

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

func recieveStringMessageFromServer(conn net.Conn) string {
	messagercv, err := bufio.NewReader(conn).ReadString('\n')
	check(err)
	messagercv = strings.TrimSpace(messagercv)
	return messagercv
}

func findSuccessor(id uint32) string {
	lookUpSuccID := thisPeer.succID
	if thisPeer.succID < thisPeer.id {
		lookUpSuccID += modulo
	}
	if thisPeer.predID == thisPeer.succID {
		if thisPeer.id < thisPeer.succID {
			if id > thisPeer.id && id <= thisPeer.succID {
				// Its succ
				return thisPeer.succIP + ":" + thisPeer.succPort
			} else {
				// Its me
				return thisPeer.ip + ":" + thisPeer.port
			}
		} else {
			if id > thisPeer.id && id <= (thisPeer.succID+modulo) {
				// Its succ
				return thisPeer.succIP + ":" + thisPeer.succPort
			} else {
				// Its me
				return thisPeer.ip + ":" + thisPeer.port
			}
		}
	} else {
		if thisPeer.predIP != "" && (thisPeer.predID < id && id <= thisPeer.id) {
			// I have it, give my info
			return thisPeer.ip + ":" + thisPeer.port
		} else if thisPeer.id < id && id <= lookUpSuccID {
			// My successor has it, give its info
			return thisPeer.succIP + ":" + thisPeer.succPort
		} else {
			// I dont know, ask my successor
			connSucc, err := net.Dial("tcp", thisPeer.succIP+":"+thisPeer.succPort)
			check(err)
			idStr := strconv.Itoa(int(id))
			check(err)
			connSucc.Write([]byte("F" + idStr + "\n")) // asking my successor here
			// this request will go through the ring and returned back
			reader := bufio.NewReader(connSucc)
			ipandport, err := reader.ReadString('\n')
			check(err)
			ipandport = strings.TrimSpace(ipandport)
			return ipandport
		}
	}
}

// Goroutine #1
func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
		messageTypeByte, _ := reader.ReadByte()
		switch messageTypeByte {
		case []byte("S")[0]: // S means Update my successor
			// ip:port,\n
			// Read ip:port
			ipandport, err := reader.ReadString('\n')
			check(err)
			ipandport = strings.TrimSpace(ipandport)
			ip := strings.Split(ipandport, ":")[0]
			port := strings.Split(ipandport, ":")[1]
			id := hash(ipandport)
			thisPeer.succID = id
			thisPeer.succIP = ip
			thisPeer.succPort = port
			conn.Write([]byte("1"))
			return
		case []byte("P")[0]: // P means Update my predecessor
			// ip:port,\n
			// Read ip:port
			ipandport, err := reader.ReadString('\n')
			check(err)
			ipandport = strings.TrimSpace(ipandport)
			ip := strings.Split(ipandport, ":")[0]
			port := strings.Split(ipandport, ":")[1]
			id := hash(ipandport)
			thisPeer.predID = id
			thisPeer.predIP = ip
			thisPeer.predPort = port
			conn.Write([]byte("1"))
			return
		case []byte("G")[0]: // G means Get predecessor
			conn.Write([]byte(thisPeer.predIP + ":" + thisPeer.predPort + "\n"))
			return
		case []byte("T")[0]: // T means Recieve files (Transfer for letter)
			// Read filename
			fileName, err := reader.ReadString('\n')
			check(err)
			fileName = strings.TrimSpace(fileName)
			// Read file bytes length
			byteCountStr, err := reader.ReadString('\n')
			check(err)
			byteCountStr = strings.TrimSpace(byteCountStr)
			byteCount, err := strconv.Atoi(byteCountStr)
			check(err)
			fmt.Println("\nFile recieved: " + fileName + " (" + byteCountStr + "B)")
			// Read bytes
			var bytes []byte
			for i := 0; i < byteCount; i++ {
				b, err := reader.ReadByte()
				check(err)
				bytes = append(bytes, b)
			}
			// Write file to the local storage
			err = ioutil.WriteFile(fileName, bytes, 0644)
			check(err)
			// Respond success
			conn.Write([]byte("1"))
			return
		case []byte("M")[0]: // M means Migrate files
			// expect ipport,\n
			// you will transfer files with key <= hash(ipport) to that server
			ipport, err := reader.ReadString('\n')
			check(err)
			ipport = strings.TrimSpace(ipport)
			targetPeerKey := hash(ipport)
			// start transfer
			files, err := ioutil.ReadDir(".") // read current folder
			check(err)
			for _, file := range files {
				if file.Name() != "task2-peer.go" {
					// check key
					fileKey := hash(file.Name())
					if fileKey <= targetPeerKey {
						dat, err := ioutil.ReadFile(file.Name())
						check(err)
						datLen := len(dat)
						bytesToSend := append([]byte("T"+file.Name()+"\n"), []byte(strconv.Itoa(datLen) + "\n")[:]...)
						bytesToSend = append(bytesToSend, dat[:]...)
						connPeer, err := net.Dial("tcp", ipport)
						check(err)
						connPeer.Write(bytesToSend)
						readerPeer := bufio.NewReader(connPeer)
						messageTypeByte, _ := readerPeer.ReadByte()
						if messageTypeByte != []byte("1")[0] {
							check(errors.New("error during file transfer"))
						}
						// now remove file from your own directory
						err = os.Remove(file.Name())
						check(err)
					}

				}
			}
		case []byte("F")[0]: // F means Find successor
			idStr, err := reader.ReadString('\n')
			//fmt.Println("\n\tIDSTR: " + idStr + " is this.")
			check(err)
			idStr = strings.TrimSpace(idStr)
			idInt, err2 := strconv.Atoi(idStr)
			check(err2)
			id := uint32(idInt)
			if thisPeer.predIP != "" && (thisPeer.predID < id && id <= thisPeer.id) {
				// I have it, give my info
				conn.Write([]byte(thisPeer.ip + ":" + thisPeer.port + "\n"))
			} else if thisPeer.id < id && id <= thisPeer.succID {
				// My successor has it, give its info
				conn.Write([]byte(thisPeer.succIP + ":" + thisPeer.succPort + "\n"))
			} else {
				// I dont know, ask my successor
				connSucc, err := net.Dial("tcp", thisPeer.succIP+":"+thisPeer.succPort)
				check(err)
				connSucc.Write([]byte("F" + idStr + "\n")) // expect response
				readerSucc := bufio.NewReader(connSucc)
				ipandport, err := readerSucc.ReadString('\n')
				check(err)
				ipandport = strings.TrimSpace(ipandport)
				conn.Write([]byte(ipandport + "\n"))
				connSucc.Close()
			}
			return
		case []byte("1")[0]: // 1 Client sent a file
			// File is coming from client
			// \n - filename - \n - filebytes
			// Read filename
			fileName, err := reader.ReadString('\n')
			check(err)
			fileName = strings.TrimSpace(fileName)
			// Read file bytes length
			byteCountStr, err := reader.ReadString('\n')
			check(err)
			byteCountStr = strings.TrimSpace(byteCountStr)
			byteCount, err := strconv.Atoi(byteCountStr)
			check(err)
			fmt.Print("\nFile recieved: " + fileName + " (" + byteCountStr + "B)")
			// Read bytes
			var bytes []byte
			for i := 0; i < byteCount; i++ {
				b, err := reader.ReadByte()
				check(err)
				bytes = append(bytes, b)
			}
			// Write file to the local storage
			err = ioutil.WriteFile(fileName, bytes, 0644)
			check(err)
			// Respond
			conn.Write([]byte("File " + fileName + " is uploaded and stored on server.\n"))
		case []byte("2")[0]: // 2 Client requested a file
			// Read filename
			filename, err := reader.ReadString('\n')
			check(err)
			filename = strings.TrimSpace(filename)
			// Check file exists
			if fileExists(filename) {
				// Read file from local
				dat, err := ioutil.ReadFile(filename)
				check(err)
				datLen := len(dat)
				datLenStr := strconv.Itoa(datLen)
				check(err)
				fmt.Println("File requested " + filename + " (" + datLenStr + "B)")
				// Send it
				bytesToSend := append([]byte("1"+datLenStr+"\n"), dat[:]...)
				conn.Write(bytesToSend)
			} else {
				conn.Write([]byte("2No such file...\n"))
			}
		default:
		}
	}
}

// Goroutine #2
func terminal() {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("\t1) Enter the peer address to connect\n\t2) Enter the key to find its successor\n\t3) Enter the filename to take its hash\n\t4) Display my ID, successors ID and predecessors ID\n\t5) Display the stored filenames and their keys\n\t6) Shutdown this peer\n>Please select an option: ")

		// Small bug workaround
		if thisPeer.predID == thisPeer.id && thisPeer.succID == thisPeer.id {
			thisPeer.predIP = ""
			thisPeer.succIP = ""
		}
		scanner.Scan()
		input := scanner.Text()

		switch input {
		case "1": // Add a new peer to the ring
			// Input ip:port
			fmt.Print(">Enter the peer address (ip:port): ")
			scanner.Scan()
			ipandport := scanner.Text()
			ipandport = strings.TrimSpace(ipandport)
			// Calculate hash (id)
			newpeerID := hash(ipandport)
			/*
				Cases:
				new? --- pred --- new? --- me --- new? --- succ --- new?

				Assumptions:
				My successors predecessor is always me
				My predecessors successor is always me
				pred and succ are both nil or both not nil (cant be one nil and one not nil)
				pred and succ are both nil if I am the only one in the ring
			*/
			if thisPeer.predIP == "" && thisPeer.succIP == "" {
				// No one in the ring yet.
				thisPeer.succID = newpeerID
				thisPeer.succIP = strings.Split(ipandport, ":")[0]
				thisPeer.succPort = strings.Split(ipandport, ":")[1]
				thisPeer.predID = newpeerID
				thisPeer.predIP = strings.Split(ipandport, ":")[0]
				thisPeer.predPort = strings.Split(ipandport, ":")[1]
				// Notify successor (which is also the predecessor)
				connPeer, err := net.Dial("tcp", ipandport)
				check(err)
				readerPeer := bufio.NewReader(connPeer)
				connPeer.Write([]byte("S" + thisPeer.ip + ":" + thisPeer.port + "\n"))
				messageTypeByte1, _ := readerPeer.ReadByte()
				if messageTypeByte1 != []byte("1")[0] {
					check(errors.New("error setting succ of peer"))
				}
				connPeer2, err := net.Dial("tcp", ipandport)
				check(err)
				readerPeer2 := bufio.NewReader(connPeer2)
				connPeer2.Write([]byte("P" + thisPeer.ip + ":" + thisPeer.port + "\n"))
				messageTypeByte2, _ := readerPeer2.ReadByte()
				if messageTypeByte2 != []byte("1")[0] {
					check(errors.New("error setting pred of peer"))
				}
				// Migrate files from me to new peer
				// BUG: cant differentiate betwen 0 vs modulo itself
				connMe, err := net.Dial("tcp", thisPeer.ip+":"+thisPeer.port) // connect to myself actually
				check(err)
				connMe.Write([]byte("M" + ipandport + "\n")) // give the address of new guy for migration

			} else {
				newPeerIP := strings.Split(ipandport, ":")[0]
				newPeerPort := strings.Split(ipandport, ":")[1]
				// Find successor
				ipandportSucc := findSuccessor(newpeerID)
				fmt.Printf("Successor of the new peer id %d is %s with id %d\n", newpeerID, ipandportSucc, hash(ipandportSucc))
				// Migrate files from successor to newpeer
				connSuccM, err := net.Dial("tcp", ipandportSucc)
				check(err)
				connSuccM.Write([]byte("M" + ipandport + "\n"))
				// Get predecessor of my successor
				connSucc, err := net.Dial("tcp", ipandportSucc)
				check(err)
				connSucc.Write([]byte("G"))
				readerSucc := bufio.NewReader(connSucc)
				ipandportPredOfSucc, err := readerSucc.ReadString('\n')
				check(err)
				ipandportPredOfSucc = strings.TrimSpace(ipandportPredOfSucc)
				// Set predeccessor of succesor to me (while getting its data)
				connSucc2, err := net.Dial("tcp", ipandportSucc)
				check(err)
				connSucc2.Write([]byte("P" + newPeerIP + ":" + newPeerPort + "\n"))
				readerSucc2 := bufio.NewReader(connSucc2)
				messageTypeByte1, _ := readerSucc2.ReadByte()
				if messageTypeByte1 != []byte("1")[0] {
					check(errors.New("error setting pred of succ"))
				}
				// Set successor of that predecessor to me
				connPred, err := net.Dial("tcp", ipandportPredOfSucc)
				check(err)
				connPred.Write([]byte("S" + newPeerIP + ":" + newPeerPort + "\n"))
				readerPred := bufio.NewReader(connPred)
				messageTypeByte2, _ := readerPred.ReadByte()
				if messageTypeByte2 != []byte("1")[0] {
					check(errors.New("error setting succ of pred"))
				}
				// Notify myself (the new peer) to update the successor
				connNew1, err := net.Dial("tcp", ipandport)
				check(err)
				connNew1.Write([]byte("S" + ipandportSucc + "\n"))
				readerNew1 := bufio.NewReader(connNew1)
				messageTypeByte3, _ := readerNew1.ReadByte()
				if messageTypeByte3 != []byte("1")[0] {
					check(errors.New("error setting succ of new peer"))
				}
				// Notify myself (the new peer) to update the predecessor
				connNew2, err := net.Dial("tcp", ipandport)
				check(err)
				connNew2.Write([]byte("P" + ipandportPredOfSucc + "\n"))
				readerNew2 := bufio.NewReader(connNew2)
				messageTypeByte4, _ := readerNew2.ReadByte()
				if messageTypeByte4 != []byte("1")[0] {
					check(errors.New("error setting pred of new peer"))
				}
			}

		case "2": // Find successor of the key
			fmt.Print(">Enter the key to find the successor: ")
			scanner.Scan()
			key := scanner.Text()
			key = strings.TrimSpace(key)
			keyInt, err := strconv.Atoi(key)
			check(err)
			// Call the function
			ipandportresponse := findSuccessor(uint32(keyInt))
			fmt.Printf("Successor is: %s with peer id %d\n", ipandportresponse, hash(ipandportresponse))
		case "3": // Calculate hash (no lookup) of a filename
			fmt.Print(">Enter the filename to send: ")
			scanner.Scan()
			filename := scanner.Text()
			filename = strings.TrimSpace(filename)
			// Print hash value
			fmt.Print("Hash value of " + filename + ": ")
			fmt.Println(hash(filename))
		case "4": // Display struct details
			if thisPeer.predID == 999 && thisPeer.succID == 999 {
				fmt.Printf("My id: %d I don't have any neighbor peers. \n", thisPeer.id)
			} else {
				fmt.Printf("My id: %d\tPred id: %d\tSucc id: %d\n", thisPeer.id, thisPeer.predID, thisPeer.succID)
			}
		case "5": // Show each file I own
			files, err := ioutil.ReadDir(".") // read current folder
			check(err)
			fmt.Println("Files in my directory:")
			for _, file := range files {
				if file.Name() != "task2-peer.go" {
					fmt.Printf("\t%s -> %d\n", file.Name(), hash(file.Name()))
				}
			}
			fmt.Print("\n")
		case "6": // Close peer
			// Transfer each file I own to the successor
			files, err := ioutil.ReadDir(".") // read current folder
			check(err)
			for _, file := range files {
				if file.Name() != "task2-peer.go" {
					dat, err := ioutil.ReadFile(file.Name())
					check(err)
					datLen := len(dat)
					bytesToSend := append([]byte("T"+file.Name()+"\n"), []byte(strconv.Itoa(datLen) + "\n")[:]...)
					bytesToSend = append(bytesToSend, dat[:]...)
					connSucc, err := net.Dial("tcp", thisPeer.succIP+":"+thisPeer.succPort)
					check(err)
					connSucc.Write(bytesToSend)
					readerSucc := bufio.NewReader(connSucc)
					messageTypeByte, _ := readerSucc.ReadByte()
					if messageTypeByte != []byte("1")[0] {
						check(errors.New("error during file transfer"))
					}
					// now remove file from your own directory
					err = os.Remove(file.Name())
					check(err)
				}
			}
			// Notify predecessor to update its successor to my successor
			connPred, err := net.Dial("tcp", thisPeer.predIP+":"+thisPeer.predPort)
			check(err)
			connPred.Write([]byte("S" + thisPeer.succIP + ":" + thisPeer.succPort + "\n"))
			readerPred := bufio.NewReader(connPred)
			messageTypeByte, _ := readerPred.ReadByte()
			if messageTypeByte != []byte("1")[0] {
				check(errors.New("error setting pred of new peer"))
			}
			// Notify successor to update its predecessor to my predecessor
			connSucc, err := net.Dial("tcp", thisPeer.succIP+":"+thisPeer.succPort)
			check(err)
			connSucc.Write([]byte("P" + thisPeer.predIP + ":" + thisPeer.predPort + "\n"))
			readerSucc := bufio.NewReader(connSucc)
			messageTypeByte2, _ := readerSucc.ReadByte()
			if messageTypeByte2 != []byte("1")[0] {
				check(errors.New("error setting pred of new peer"))
			}
			// Exit
			os.Exit(0)
		default:
			//fmt.Println("Invalid option.")
		}

	}
}

func main() {
	// Command line arguments
	argsWithoutProg := os.Args[1:]
	var listenToPort, listenToIP, ipandport string

	if len(os.Args) == 1 {
		listenToIP = "localhost" // localhost
		listenToPort = "8080"
	} else {
		listenToIP = "0.0.0.0" // change this to 0.0.0.0 before AWS anywhere
		listenToPort = argsWithoutProg[0]
	}

	ipandport = listenToIP + ":" + listenToPort
	fmt.Println("> Launching the server listening at " + ipandport)

	// make bigger to avoid clashes of id's
	modulo = uint32(math.Pow(2.0, 10.0)) // this determines the size of my logical ring, it is a global variable

	// Initalize peer object
	thisPeer.id = hash("54.172.25.59:"+listenToPort)
	thisPeer.ip = listenToIP
	thisPeer.port = listenToPort
	thisPeer.predIP = ""
	thisPeer.succIP = ""
	thisPeer.predPort = ""
	thisPeer.succPort = ""
	thisPeer.predID = 999
	thisPeer.succID = 999

	go terminal() // Start goroutine for the terminal

	ln, err := net.Listen("tcp", ipandport) // listening to incoming connections
	check(err)

	for { // loop forever (or until ctrl-c)

		conn, err := ln.Accept() // accept connection on port
		//fmt.Println("\t> Client connected: ", conn.RemoteAddr().String())
		check(err)
		// print this so it is seen after a client connects too
		//fmt.Print("\n\t1) Enter the peer address to connect\n\t2) Enter the key to find its successor\n\t3) Enter the filename to take its hash\n\t4) Display my ID, successors ID and predecessors ID\n\t5) Display the stored filenames and their keys\n\t6) Shutdown this peer\n>Please select an option: ")

		go handleConnection(conn)

	}
}
