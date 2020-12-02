package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"log"
	"math"
	"net"
	"os"
	"context"
	"google.golang.org/grpc"
	pb "Lab2-Test/Tarea2-SD/pipeline"
)

type Server struct {
	pb.UnimplementedGreeterServer
}

/*-----------------------------------------------------------------------------------------*/

func (s *Server) SayHello(ctx context.Context, in *pb.Book) (*pb.Test, error) {
	req := int(in.Request)
	log.Printf("Se solicitará el chunk: %d ", req)
	auxiliar := sendChunk((req), in.BookName)
	return &pb.Test{Valor: in.Request, Chuck: auxiliar}, nil
}

/*-----------------------------------------------------------------------------------------*/
func (s *Server) ClientToDataNode(ctx context.Context, in *pb.DataChuck) (*pb.Resultado, error) {
	fileName := "" + "_" + strconv.FormatUint(uint64(in.Valor), 10)
	_, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ioutil.WriteFile(fileName, in.Chunk, os.ModeAppend)
	fmt.Println("Split to : ", fileName)
	return &pb.Resultado{Valor: in.Valor}, nil
}


/*-----------------------------------------------------------------------------------------*/
func clientsReception() {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", 50054))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterGreeterServer(grpcServer, &Server{})

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}

/*-----------------------------------------------------------------------------------------*/

func gutTheFile(FileName string) uint64 {
	fileToBeChunked := FileName
	file, err := os.Open(fileToBeChunked)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 256000 //Bytes

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)
		fileName := FileName + "_" + strconv.FormatUint(i, 10)
		_, err := os.Create(fileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)

		fmt.Println("Split to : ", fileName)
	}
	return totalPartsNum
}

/**---------------------------------------------------------------------------------------------wwww*/
func sendChunk(partToSend int, bookName string) []byte {
	gutTheFile(bookName)
	chunkToSend := bookName + "_" + strconv.FormatUint(uint64(partToSend), 10)
	chunkBytes, err := ioutil.ReadFile(chunkToSend) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}
	return chunkBytes
}

/*-----------------------------------------------------------------------------------------*/

func main() {
	go clientsReception()
	opcion := 0
	for opcion != -1 {
		fmt.Println("Ingrese -1 para cerrar el programa ")
		fmt.Scanf("%d", &opcion)
	}
}
