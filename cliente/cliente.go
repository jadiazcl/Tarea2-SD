package main

import (
	pb "Lab2/Tarea2-SD/pipeline"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// func requestChunk(idMchn int) {

// 	machines := []string{"dist157", "dist158", "dist159", "dist160"}
// 	var conn *grpc.ClientConn
// 	mchn := machines[idMchn]
// 	conn, err := grpc.Dial(mchn+":50054", grpc.WithInsecure())
// 	if err != nil {
// 		log.Fatalf("did not connect: %s", err)
// 	}
// 	fileChunk := 0
// 	defer conn.Close()
// 	fmt.Println("waiting >>>")
// 	fmt.Scanf("%d", &fileChunk)
// 	c := pb.NewGreeterClient(conn)
// 	response, err := c.SayHello(context.Background(), &pb.Solcamion{IdCamion: int32(fileChunk)})

// 	if err != nil {
// 		log.Fatalf("Error when calling SayHello: %s", err)
// 	}
// 	log.Printf("La parte solicitada es: %d", response.Valor)
// 	fileName := "bigfile_" + strconv.FormatUint(uint64(fileChunk), 10)
// 	ioutil.WriteFile(fileName, response.Chuck, os.ModeAppend)
// 	//REARMAR
// }
func requestChunk(idMchn int, bookName string) {

	machines := []string{"dist157", "dist158", "dist159", "dist160"}
	var conn *grpc.ClientConn
	mchn := machines[idMchn]
	log.Println("maquina", mchn)
	conn, err := grpc.Dial(mchn+":50054", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	fileChunk := 0
	defer conn.Close()
	fmt.Println("waiting >>>")
	fmt.Scanf("%d", &fileChunk)
	c := pb.NewGreeterClient(conn)
	response, err := c.SayHello(context.Background(), &pb.Book{Request: int32(fileChunk), BookName: bookName})

	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}
	log.Printf("La parte solicitada es: %d", response.Valor)
	fileName := "bigfile_" + strconv.FormatUint(uint64(fileChunk), 10)
	ioutil.WriteFile(fileName, response.Chuck, os.ModeAppend)
	//REARMAR
}

func main() {
	requestChunk(1, "test.pdf")
	//requestChunk(2, "test.pdf")
	//requestChunk(3, "test.pdf")
	// requestChunk(1, "test.pdf")
	// requestChunk(2, "test.pdf")

}
