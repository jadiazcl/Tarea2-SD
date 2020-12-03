package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"log"
	"math"
	"net"
	"math/rand"
	"os"
	"context"
	"google.golang.org/grpc"
	pb "Lab2-Test/Tarea2-SD/pipeline"
)

type Server struct {
	pb.UnimplementedGreeterServer
}

/*-----------------------------------------------------------------------------------------*/

func (s *Server) YadaYada(ctx context.Context, in *pb.ClientCheck) (*pb.Resultado, error) {
	maquina := int(in.Request)
	nom := in.BookName
	partes := int(in.Partes)
	auxiliar := createDistribution(partes,maquina)
	valor:=EnviarDistribucion(maquina,auxiliar,partes,nom)

	return &pb.Resultado{Valor: int32(valor)}, nil
}

/*-----------------------------------------------------------------------------------------*/

func (s *Server) SayHello(ctx context.Context, in *pb.Book) (*pb.Test, error) {
	req := int(in.Request)
	log.Printf("Se solicitará el chunk: %d ", req)
	auxiliar := sendChunk((req), in.BookName)
	return &pb.Test{Valor: in.Request, Chuck: auxiliar}, nil
}
/*-----------------------------------------------------------------------------------------*/
func (s *Server) TesteoEstado(ctx context.Context, in *pb.Bla) (*pb.Bla, error) {
	req := int(in.Valor)
	log.Printf("Se solicitará el chunk: %d ", req)
	return &pb.Bla{Valor:int32(1)}, nil
}

/*-----------------------------------------------------------------------------------------*/
func (s *Server) ClientToDataNode(ctx context.Context, in *pb.DataChuck) (*pb.Resultado, error) {
	fileName := in.NombreArchivo + "_" + strconv.FormatUint(uint64(in.Valor), 10)
	_, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ioutil.WriteFile(fileName, in.Chunck, os.ModeAppend)
	fmt.Println("Split to : ", fileName)
	return &pb.Resultado{Valor: in.Valor}, nil
}

/*-----------------------------------------------------------------------------------------*/
func EnviarDistribucion(maquina int, distribucion string, partes int,bookTag string ) string{
	maquinas:=strings.Split(distribucion, "-")
	m := [3]string{"dist158", "dist159", "dist160"}
	for index := 0;  < len(maquinas)-1; ++ {
		if maquinas[index]!=m[maquina]{
			var conn *grpc.ClientConn
			conn, err := grpc.Dial("dist157:50055", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("did not connect: %s", err)
			}
			defer conn.Close()
			chunkToSend := bookTag + "_" + strconv.FormatUint(uint64(index), 10)
			chunkBytes, err := ioutil.ReadFile(chunkToSend) // just pass the file name
			if err != nil {
				fmt.Print(err)
			}
			c := pb.NewGreeterClient(conn)
			response, err := c.ClientToDataNode(context.Background(), &pb.DataChuck{Valor: int32(index), Chunck: chunkBytes,NombreArchivo:bookTag})
			if err != nil {
				log.Fatalf("Error when enviar distribucion: %s", err)
			}
			return response.Proposal
		}
		fmt.Println("Parte enviada")
	}
}

/*-----------------------------------------------------------------------------------------*/
func EnviarPartes(distribucion string, nombre_archivo string, maquina int  ) string{

	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist157:50055", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)
	response, err := c.CheckDistribucion(context.Background(), &pb.Distribution{Proposal: distribucion, BookName: bookTag,Partes: int32(partes),Maquina: int32(maquina)})
	if err != nil {
		log.Fatalf("Error when enviar distribucion: %s", err)
	}
	return response.Proposal
}


/*-----------------------------------------------------------------------------------------*/
func createDistribution(numParts int,  maquina int ) string {
	m := [3]string{"dist158", "dist159", "dist160"}
	aux:=m[maquina]	+"-"
	cantidad:=1
	for i := 0; i < 3; i++ {
		if i!=maquina{
			if cantidad<numParts{
				cantidad=cantidad+1
				aux=aux+m[i]+"-"
			}
		}
	}
	if cantidad<numParts{
		for j := cantidad; j<numParts; j++ {
			randomIndex := rand.Intn(len(m))
			pick := m[randomIndex]
			aux=aux+pick+"-"
		}
	}
	fmt.Println("Distribution")
	fmt.Println(aux)
	return aux
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
