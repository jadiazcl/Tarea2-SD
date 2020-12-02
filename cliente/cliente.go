package main

import (
	pb "Lab2-Centralizada/Tarea2-SD/pipeline"
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

type Server struct {
	pb.UnimplementedGreeterServer
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

func (s *Server) SayHello(ctx context.Context, in *pb.Book) (*pb.Test, error) {
	req := int(in.Request)
	log.Printf("Se solicitará el chunk: %d ", req)
	auxiliar := sendChunk((req), in.BookName)
	parts, _ := howManyChunks(in.BookName)

	return &pb.Test{Valor: in.Request, Chunk: auxiliar, Parts: int32(parts)}, nil
}

func recepcion_clientes() {
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

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/*||||||||||||||||||||||||||||||||||||||||||||||||||||||  CLIENTE UPLOADER  ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

func howManyChunks(FileName string) (uint64, uint64) {
	fileToBeChunked := FileName
	file, err := os.Open(fileToBeChunked)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	fileSize := uint64(fileInfo.Size())
	fileChunk := 256000 //Bytes
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)
	return totalPartsNum, fileSize
}

/*----------------------------------------------------------------------------------------------------------------------------------------*/
// Esta función separa el archivo en diferentes archivos de 250 KB cada uno
/*----------------------------------------------------------------------------------------------------------------------------------------*/
func gutTheFile(fileName string) uint64 {
	fileChunk := 256000 //Bytes

	totalPartsNum, fileSize := howManyChunks(fileName)
	for i := uint64(0); i < totalPartsNum; i++ {
		partSize := int(math.Min(float64(fileChunk), float64(int64(fileSize)-int64(i*uint64(fileChunk)))))
		partBuffer := make([]byte, partSize)
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println()
			os.Exit(1)

		}
		file.Read(partBuffer)
		fileName := fileName + "_" + strconv.FormatUint(i, 10)
		_, err1 := os.Create(fileName)
		if err != nil {
			fmt.Println(err1)
			os.Exit(1)
		}
		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
		fmt.Println("Split to : ", fileName)
	}
	return totalPartsNum
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
/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
func pedir_archivo() (int, string, string) {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist157:50055", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	opcion := ""
	defer conn.Close()
	fmt.Println("Ingrese el nombre del pdf a pedir")
	fmt.Scanf("%s", &opcion)
	c := pb.NewGreeterClient(conn)
	response, err := c.SolicitarUbicaciones(context.Background(), &pb.ConsultaUbicacion{NombreArchivo: opcion})
	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}
	partes := response.Partes
	ubicacion := response.Ubicaciones
	return int(partes), ubicacion, opcion
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/*||||||||||||||||||||||||||||||||||||||||||||||||||||||  CLIENTE DOWNLOADER  ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

/*----------------------------------------------------------------------------------------------------------------------------------------*/
// Esta función se conecta a cierto nodo para recuperar cierto chunk de un archivo
/*----------------------------------------------------------------------------------------------------------------------------------------*/
func requestChunk(maquina string, fileChunk int, bookTag string) {

	var conn *grpc.ClientConn
	log.Println("maquina", maquina)
	conn, err := grpc.Dial(maquina+":50054", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()
	fmt.Println("waiting >>>")
	fmt.Println("*     Chunk Solicitado      *")
	fmt.Println(fileChunk)
	fmt.Println("*****************************")
	c := pb.NewGreeterClient(conn)
	response, err := c.SayHello(context.Background(), &pb.Book{Request: int32(fileChunk), BookName: bookTag})
	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}
	log.Printf("La parte solicitada es: %d", response.Valor)
	fileName := bookTag + "_" + strconv.FormatUint(uint64(fileChunk), 10)
	fmt.Println("se recibe: ", fileName)
	ioutil.WriteFile(fileName, response.Chunk, os.ModeAppend)

}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/
/*----------------------------------------------------------------------------------------------------------------------------------------*/
// Esta función conecta los chunks ya recogidos en un solo archivo
/*----------------------------------------------------------------------------------------------------------------------------------------*/
func stitchTheFile(originalName string, totalPartsNum uint64) {
	writePosition := int64(0)
	newFileName := "NEW_" + originalName
	file, e := os.Create(newFileName)
	if e != nil {
		log.Fatal(e)
	}
	for j := uint64(0); j < totalPartsNum; j++ {
		currentChunkFileName := originalName + "_" + strconv.FormatUint(j, 10)
		newFileChunk, err := os.Open(currentChunkFileName)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer newFileChunk.Close()
		chunkInfo, err := newFileChunk.Stat()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		chunkSize := chunkInfo.Size()
		chunkBufferBytes := make([]byte, chunkSize)
		fmt.Println("Appending at position : [", writePosition, "] bytes")
		writePosition = writePosition + chunkSize
		reader := bufio.NewReader(newFileChunk)
		_, err = reader.Read(chunkBufferBytes)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		n, err := file.Write(chunkBufferBytes)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		file.Sync()
		chunkBufferBytes = nil
		fmt.Println("Written ", n, " bytes")
		fmt.Println("Recombining part [", j, "] into : ", newFileName)
	}
	file.Close()
}

/*||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||*/

func main() {
	// partes, maquinas, nameFile := pedir_archivo()
	// aux_maquina := strings.Split(maquinas, "-")
	// totalChunks := uint64(partes)
	// aux := 0
	// for j := uint64(0); j < totalChunks; j++ {
	// 	aux = int(j)
	// 	requestChunk(aux_maquina[aux], aux, nameFile)

	// }
	// stitchTheFile(nameFile, totalChunks)
	go recepcion_clientes()
	opcion := "-"
	op := 1
	fmt.Printf(" Nombre archivo : ")
	fmt.Scanf("%s", &opcion)
	totalParts := gutTheFile(opcion)
	fmt.Printf(" continue?\n  yes :1  no: anything else ")

	fmt.Scanf("%d", &op)

	if op != 1 {
		fmt.Printf(" continue?\n  yes :1  no: anything else ")
		fmt.Scanf("%d", &op)

	}
}
