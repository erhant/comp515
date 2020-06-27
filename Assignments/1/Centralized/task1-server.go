package main

// tmux diye bir sey var, serverdaki terminali kapadiginda programin kapanmamasini sagliyor. NodeJS'teki pm2 gibi bir sey yani.

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
)

func check(e error) {
	if e != nil {
		//fmt.Println(e)
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

func constructFilename(username string, filename string) string {
	return username + "__" + filename
}
func handleConnection(conn net.Conn) { //net.Conn is the type
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
		messageTypeByte, _ := reader.ReadByte() // will listen for message to process ending in newline (\n)
		switch messageTypeByte {
		case []byte("1")[0]:
			// User login
			// username - \n
			clientUsername, err := reader.ReadString('\n')
			check(err)
			clientUsername = strings.TrimSpace(clientUsername)
			fmt.Println("User " + clientUsername + " has connected.")
			// Respond
			conn.Write([]byte("Welcome, " + clientUsername + ".\n")) // writing back to client
		case []byte("2")[0]:
			// File is coming from client
			// username - \n - filename - \n - filebytes
			/// Read username
			clientUsername, err := reader.ReadString('\n')
			check(err)
			clientUsername = strings.TrimSpace(clientUsername)
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
			fmt.Println("User " + clientUsername + " has sent a file " + fileName + " (" + byteCountStr + "B)")
			// Read bytes
			var bytes []byte
			for i := 0; i < byteCount; i++ {
				b, err := reader.ReadByte()
				check(err)
				bytes = append(bytes, b)
			}
			// Write file to the local storage
			err = ioutil.WriteFile(constructFilename(clientUsername, fileName), bytes, 0644)
			check(err)
			// Respond
			conn.Write([]byte("File " + fileName + " of user " + clientUsername + " uploaded and stored on server.\n"))
			fmt.Println("File has been stored and response has been made.")
		case []byte("3")[0]:
			// username - \n - filename - \n
			/// Read username
			clientUsername, err := reader.ReadString('\n')
			check(err)
			clientUsername = strings.TrimSpace(clientUsername)
			// Read filename
			filename, err := reader.ReadString('\n')
			check(err)
			filename = strings.TrimSpace(filename)
			// Check file exists
			if fileExists(constructFilename(clientUsername, filename)) {
				// Read file from local
				dat, err := ioutil.ReadFile(constructFilename(clientUsername, filename))
				check(err)
				datLen := len(dat)
				datLenStr := strconv.Itoa(datLen)
				check(err)
				fmt.Println("User " + clientUsername + " has requested the file " + filename + " (" + datLenStr + "B)")
				// Send it
				bytesToSend := append([]byte("1"+datLenStr+"\n"), dat[:]...)
				conn.Write(bytesToSend)
			} else {
				conn.Write([]byte("2No such file...\n"))
			}

		case []byte("4")[0]:
			// User exit
			fmt.Println("> Client disconnected: " + conn.RemoteAddr().String())
			conn.Write([]byte("Bye bye!\n"))
			return // connection close is deferred anyways
		default:
			//fmt.Println("Unknown command recieved: " + string(messageTypeByte))
			//conn.Write([]byte("I dont know that command.\n"))
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
		listenToIP = "0.0.0.0" // anywhere
		listenToPort = argsWithoutProg[0]
	}
	ipandport = listenToIP + ":" + listenToPort
	fmt.Println("> Launching the server listening at " + ipandport)

	ln, err := net.Listen("tcp", ipandport) // listening to incoming connections
	check(err)

	for { // loop forever (or until ctrl-c)

		conn, err := ln.Accept() // accept connection on port
		fmt.Println("> Client connected: ", conn.RemoteAddr().String())
		if err != nil {
			check(err)
			continue
		}

		go handleConnection(conn)

	}
}
