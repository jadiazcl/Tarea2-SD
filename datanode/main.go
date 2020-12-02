package main

import (
	pb "Lab2-Centralizada/Tarea2-SD/pipeline"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"

	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedGreeterServer
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
func (s *Server) YadaYada(ctx context.Context, in *pb.Book) (*pb.Distribution, error) {
	req := int(in.Request)
	nm := in.BookName
	auxiliar := createDistribution(req, nm)
	return &pb.Distribution{Proposal: auxiliar}, nil
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
// func (s *Server) SayHello(ctx context.Context, in *pb.Book) (*pb.Test, error) {
// 	req := int(in.Request)
// 	log.Printf("Se solicitará el chunk: %d ", req)
// 	auxiliar := sendChunk(req, in.BookName)
// 	return &pb.Test{Valor: in.Request, Chunk: auxiliar}, nil
// }

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

func (s *Server) ClientToDataNode(ctx context.Context, in *pb.Test) (*pb.Book, error) {
	fileName := "" + "_" + strconv.FormatUint(uint64(in.Valor), 10)
	_, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ioutil.WriteFile(fileName, in.Chunk, os.ModeAppend)
	fmt.Println("Split to : ", fileName)

	return &pb.Book{Request: in.Valor, BookName: "-"}, nil
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
func clientsReception() {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", 50055))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterGreeterServer(grpcServer, &Server{})

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
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

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

/*----------------------------------------------------------------------------------------------------------------------------------------*/
// Esta función se conecta a cierto nodo para recuperar cierto chunk de un archivo
/*----------------------------------------------------------------------------------------------------------------------------------------*/
func requestChunk(maquina string, bookTag string) {
	var conn *grpc.ClientConn
	//log.Println("maquina", maquina)
	conn, err := grpc.Dial(maquina+":50054", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()

	fmt.Println("******Chunk Solicitado*******")
	fmt.Println("**************", ChunkNum, "**************")
	fmt.Println("*****************************")
	c := pb.NewGreeterClient(conn)
	//bookTag := "newFile"
	fmt.Println(bookTag)
	response, err := c.SayHello(context.Background(), &pb.Book{Request: int32(ChunkNum), BookName: bookTag})
	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}

	log.Printf("La parte solicitada es: %d", response.Valor)
	fileName := bookTag + "_" + strconv.FormatUint(uint64(ChunkNum), 10)
	fmt.Println("se recibe: ", fileName)
	if response.Chunk != "" {
		ioutil.WriteFile(fileName, response.Chunk, os.ModeAppend)
	}
	ChunkNum++
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
//gutsTheFile retorna el total de partes
func createDistribution(numParts int, fileName string) []byte {
	ax := ""
	m := [4]string{"dist157", "dist158", "dist159", "dist160"}
	strParts := strconv.Itoa(numParts)
	c := numParts
	e := 0
	newDistr := fileName + " " + strParts + "\n"
	for i := 0; i < 4; i++ {
		ax = strconv.Itoa(i + 1)
		newDistr = newDistr + "parte_1_" + ax + " " + m[i] + "\n"
		c--
		e++
	}
	ind := 0
	if c != 0 {
		for j := c; j != 0; j-- {
			ind++
			randomIndex := rand.Intn(len(m))
			pick := m[randomIndex]
			newDistr = newDistr + "parte_" + strconv.Itoa(e+ind) + " " + pick + "\n"
		}
	}
	//fmt.Println(newDistr)
	return []byte(newDistr)
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
func sendChunk(partToSend int, bookName string) []byte {
	//gutTheFile(bookName)
	chunkToSend := bookName + "_" + strconv.FormatUint(uint64(partToSend), 10)
	chunkBytes, err := ioutil.ReadFile(chunkToSend) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}
	return chunkBytes
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

/*####################################################################################################################################### */
var ChunkNum int = 0
var FileCounter int = 1

func main() {
	bookTag := "archivo.pdf"
	go clientsReception()
	opcion := 0
	//op := 0
	total := 5
	fmt.Println("-1 : Cerrar el programa ")
	for opcion != -1 {
		for ChunkNum < total {

			requestChunk("dist157", bookTag)

			FileCounter++
		}
		fmt.Println("-1 : Cerrar el programa ")
		fmt.Scanf("%d", &opcion)
	}
	ChunkNum = 0

}
