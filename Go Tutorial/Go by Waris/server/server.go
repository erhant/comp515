package main

import (
	"bufio"
	"fmt"
	"net"
)

func handleConnection(conn net.Conn) { //net.Conn is the type

	message, _ := bufio.NewReader(conn).ReadString('\n')          // will listen for message to process ending in newline (\n)
	fmt.Print(">Server Side| Message Received:", string(message)) // printing the message on terminal
	servermessage := "Thank you for connecting."
	conn.Write([]byte(servermessage + "\n")) // writing back to client

}

func main() {
	fmt.Println("> Launching the server ...")

	ln, err := net.Listen("tcp", "localhost:8080") // listening to incoming connections

	if err != nil {
		// handle error
	}

	for { // loop forever (or until ctrl-c)

		conn, err := ln.Accept() // accept connection on port
		fmt.Println(">Server Side| New Client connected.", conn)
		if err != nil {
			// handle error
		}

		// this is single thread, which is not scalable!
		//handleConnection(conn)

		// this is multi thread (we just added the 'go' keyword!)
		// it uses goroutines
		go handleConnection(conn)

	}
}

// you can run this on terminal, and then run the client on another terminal
