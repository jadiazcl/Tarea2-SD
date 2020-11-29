package main

import (
	pb "Lab2-Test/Tarea2-SD/pipeline"
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"reflect"
)

func pedir_archivo() (int , string, string ){
   var conn *grpc.ClientConn
   conn, err := grpc.Dial("dist157:50055", grpc.WithInsecure())
   if err != nil {
     log.Fatalf("did not connect: %s", err)
   }
   opcion:=""
   defer conn.Close()
   fmt.Println("Ingrese el nombre del pdf a pedir")
   fmt.Scanf("%s", &opcion)
   c := pb.NewGreeterClient(conn)
   response, err := c.SolicitarUbicaciones(context.Background(), &pb.ConsultaUbicacion{NombreArchivo:opcion})
   if err != nil {
     log.Fatalf("Error when calling SayHello: %s", err)
   }
	 partes:=response.Partes
	 ubicacion:=response.Ubicaciones
   log.Printf("Cantidad de partes: %d", partes)
   log.Printf("Ubicacion: %s", ubicacion)
	 return int(partes),ubicacion,opcion
}

func requestChunk(maquina string, fileChunk int, bookTag string) {

	//machines := []string{"dist157", "dist158", "dist159", "dist160"}
	var conn *grpc.ClientConn
	log.Println("maquina", maquina)
	conn, err := grpc.Dial(maquina+":50054", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()
	// Esto debe ser cambiado para poder recibir todo desde un json o txt
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
	ioutil.WriteFile(fileName, response.Chuck, os.ModeAppend)
}

/*---------------------------------------------------*/
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

func main() {
	partes,maquinas,nameFile:=pedir_archivo()
	maquinas=string(maquinas)
	//aux_maquina:=""+maquinas
	aux_maquina:=strings.Split(maquinas, "-")
	fmt.Println("holanda que talca")
	fmt.Println(aux_maquina)
	fmt.Println(aux_maquina[1])
	fmt.Println(reflect.TypeOf(aux_maquina))
	totalChunks:=uint64(partes)
	aux:=0
	for j := uint64(0); j < totalChunks; j++ {
		aux=int(j)
		requestChunk(aux_maquina[aux],aux,nameFile)
	}
	stitchTheFile(nameFile, totalChunks)
}
