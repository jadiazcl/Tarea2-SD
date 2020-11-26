package main

import (
	"fmt"
	"io/ioutil"
	"strconv"

	"log"
	"math"
	"net"
	"os"

	pb "Lab2/Tarea2-SD/pipeline"
	"context"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedGreeterServer
}

/*-----------------------------------------------------------------------------------------*/

func (s *Server) SayHello(ctx context.Context, in *pb.Solcamion) (*pb.Test, error) {
	log.Printf("recibi %d ", in.IdCamion)
	auxiliar := test_archivo(in.IdCamion)
	return &pb.Test{Valor: in.IdCamion, Chuck: auxiliar}, nil
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
	fileToBeChunked := FileName // change here!
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

		// write to disk
		fileName := FileName + "_" + strconv.FormatUint(i, 10)
		_, err := os.Create(fileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// write/save buffer to disk
		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)

		fmt.Println("Split to : ", fileName)
	}
	return totalPartsNum
}

/**---------------------------------------------------------------------------------------------wwww*/
func test_archivo(partToSend int32) []byte {
	fileToBeChunked := "test.pdf" // change here!

	parts := gutTheFile(fileToBeChunked)

	chunkToSend := fileToBeChunked + "_" + strconv.FormatUint(uint64(partToSend), 10)

	// defer file.Close()

	// fileInfo, _ := file.Stat()

	// var fileSize int64 = fileInfo.Size()

	// const fileChunk = 256000

	//totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	//fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)
	//partSize := int(math.Min(fileChunk, float64(fileSize-int64(0*fileChunk)))) //parte del archivo
	b, err := ioutil.ReadFile(chunkToSend) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}
	//partBuffer := make([]byte, b)
	return b
	// just for fun, let's recombine back the chunked files in a new file
}

/*-----------------------------------------------------------------------------------------*/

func main() {
	go clientsReception()
	opcion := 0
	fmt.Println("Ingrese -1 para cerrar el programa ")
	fmt.Scanf("%d", &opcion)
	test_archivo(int32(opcion))

	for opcion != -1 {
		fmt.Println("Ingrese -1 para cerrar el programa ")
		fmt.Scanf("%d", &opcion)
	}
}
