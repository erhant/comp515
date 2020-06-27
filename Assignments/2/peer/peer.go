package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc" // this is the STAR of this code!
	"os"
	"strconv"
	"strings"
	"time"
)

var myIP, myPort, myID string
var peersToConnect []string
var peerClients []*rpc.Client
var msgBuffer []Message
var vc []int
var vi int
var peerNo int

/* EXPORTING
In go, fields and variables that start with an Uppercase letter are "Exported", and are visible to other packages. Fields that start with a lowercase letter are "unexported", and are only visible inside their own package.
The encoding/gob package depends on reflection to encode values, and can only see exported struct fields.
*/

//Message : RPC Message structure
type Message struct {
	Timestamp  []int  // Vector clock piggybacking the code
	Si         int    // Sender idx on the vector clock
	Oid        string // id of the original sender process
	Transcript string // The text that the user of the sender process has entered.
}

//Messenger : Variable for the RPC service
type Messenger int // service struct

//MessagePost : Exported message post function for the messenger
func (m *Messenger) MessagePost(message *Message, reply *int) error {
	// First command messages
	if (*message).Transcript == "__EXIT__" {
		// Graceful termination
		go termination()
		return nil
	}
	if (*message).Transcript == "__DELAY__" {
		// Delay the delivery of this messge
		msgBuffer = append(msgBuffer, *message)
		return nil
	}

	// Enqueue message that just arrived, so the code looks simpler
	msgBuffer = append(msgBuffer, *message)

	// Check if you can deliver any messages in the buffer
	deliveryHappened := true
	for deliveryHappened {
		deliveryHappened = false
		for i := 0; i < len(msgBuffer); i++ {
			// Get the message from buffer
			curMsg := msgBuffer[i]
			// Retrieve timestamp of message
			ts := curMsg.Timestamp
			si := curMsg.Si
			// Compare to your local timestamp
			var cond1, cond2 bool // deliver only if both conditions are true
			cond1 = (ts[si] == vc[si]+1)
			cond2 = true
			for j := range vc {
				if (j != si) && (ts[j] > vc[j]) {
					cond2 = false
					break
				}
			}
			if cond1 && cond2 {
				// Deliver Message
				deliveryHappened = true
				// Print contents
				fmt.Printf("VC:%s TS:%s %s: %s\n", vectorclockToString(vc), vectorclockToString(ts), curMsg.Oid, curMsg.Transcript)
				// Update local clock
				for k := range vc {
					// Take maximum on vector clocks
					vc[k] = max(vc[k], ts[k])
				}
				// Remove this message from the buffer
				msgBuffer = append(msgBuffer[:i], msgBuffer[i+1:]...) // removes the i'th element
				i--                                                   // also keep i in place because next element is in this index now
			}
		}
	}

	// Finalize
	//*reply = 1 // success
	return nil
}

/*
Main messenger program. Has several special messages:
__EXIT__  : Terminates the chat session for all peers
__VEC__   : Print the vector clock of this peer
__DELAY__ : Send a message but all peers will delay the delivery of this once (to test causal ordering)
__BUF__   : Print the contents of message buffer
*/
func messengerRoutine() {
	// Attempt to connect to all the peers in a cyclic fashion
	time.Sleep(500 * time.Millisecond)
	i := 0
	for {
		client, err := rpc.Dial("tcp", peersToConnect[i]) // connecting to the service
		if err != nil {
			// Error occured but it is ok, maybe other servers are setting up at the moment
			fmt.Printf("Dialed %s but no response...\n", peersToConnect[i])
			// Delay just a bit
			time.Sleep(500 * time.Millisecond)
		} else {
			// Connection established
			fmt.Printf("Connected to %s\n", peersToConnect[i])
			peersToConnect = append(peersToConnect[:i], peersToConnect[i+1:]...)
			peerClients = append(peerClients, client)
		}

		if len(peersToConnect) > 0 {
			i = (i + 1) % len(peersToConnect)
		} else {
			break
		}

	}
	fmt.Printf("Connected to all peers!\n\n")

	// Start the messenger (this will start on all peers at the same time thanks to the loop above)
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("You have joined the chatroom. Say hello!\n")
	var reply int
	for {
		// Scan input
		//fmt.Printf("%s: ", myID)
		scanner.Scan()
		input := scanner.Text()

		if input == "__VEC__" {
			// Print your own vector clock
			fmt.Printf("My vector clock: %s (I am index %d)\n", vectorclockToString(vc), vi)
		} else if input == "__BUF__" {
			fmt.Printf("Printing buffer contents:\n")
			for _, m := range msgBuffer {
				fmt.Printf("\tVC:%s TS:%s %s: %s\n", vectorclockToString(vc), vectorclockToString(m.Timestamp), m.Oid, m.Transcript)
			}
			fmt.Printf("Done.\n")
		} else {
			// Increment your index in the vector clock
			vc[vi]++ // note that this is the only increment case for Causally Ordered Multicasting

			// Print your own message
			//fmt.Printf("%s\t%s: %s\n", vectorclockToString(vc), myID, input)

			// Create the message
			msg := new(Message)
			msg.Oid = myID
			msg.Si = peerNo
			msg.Transcript = input
			msg.Timestamp = vc // TODO: is this copy safe?

			// Multicast the message
			for _, client := range peerClients {
				// RMI for each peer
				err := client.Call("Messenger.MessagePost", msg, &reply)
				// Confirm the reply
				checkError(err)
			}

			if input == "__EXIT__" {
				// Graceful termination
				fmt.Printf("Terminating chat session...\n")
				for _, client := range peerClients {
					// Close
					client.Close()
				}
				os.Exit(0)
			}
		}

	}
}

func main() {
	// We pass a number in the command line
	if len(os.Args) != 2 {
		fmt.Println("Usage: ", os.Args[0], "peer_number")
		os.Exit(1)
	}
	peerNo, _ = strconv.Atoi(os.Args[1])
	peerNo-- // map 1, 2, 3 .. to 0, 1, 2

	// Create and Register Messenger Service on the network
	messengerService := new(Messenger)
	rpc.Register(messengerService)

	// We read the peers from a text file, this file includes our id too
	ipandports, err := readLines("peers.txt")
	checkError(err)

	// Here we launch our server
	for i, line := range ipandports {
		if i == peerNo {
			// This is my ip and port
			myIP = strings.Split(line, "/")[0]
			myPort = strings.Split(line, "/")[1]
			myID = line
		} else {
			// This is a peer
			peerIP := strings.Split(line, "/")[0]
			peerPort := strings.Split(line, "/")[1]
			// peerID := line
			peersToConnect = append(peersToConnect, peerIP+":"+peerPort)
		}
	}

	// Our vector clock length should be same as this
	vc = make([]int, len(ipandports)) // initially all zeros
	vi = peerNo                       // this is my position in the vector clock

	// Start the server
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+myPort)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)
	fmt.Println("> Service is running!")

	// Start the terminal for messaging
	go messengerRoutine()

	// Wait for incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn) // it is important for this to be a goroutine
	}
}

// Error checking auxillary function
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

////// AUXILLARY FUNCTIONS

// credit: https://stackoverflow.com/questions/5884154/read-text-file-into-string-array-and-write
// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// Take the maximum of two numbers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Convert the timestamp to a readable string
func vectorclockToString(clock []int) string {
	// Prepare timestamp string
	tsStr := "["
	for i := 0; i < len(clock)-1; i++ { // vc is global variable

		tsStr += strconv.Itoa(clock[i])
		tsStr += ","
	}
	tsStr += strconv.Itoa(clock[len(clock)-1])
	tsStr += "]"
	return tsStr
}

func termination() {
	fmt.Printf("Terminating in a second...\n")
	time.Sleep(1000 * time.Millisecond)
	os.Exit(0)
}
