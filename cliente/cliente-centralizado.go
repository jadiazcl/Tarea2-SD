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
)

/*-----------------------------------------------------------------------------------------*/
func pedir_archivo(opcion string) (int , string, string ){
   var conn *grpc.ClientConn
   conn, err := grpc.Dial("dist157:50055", grpc.WithInsecure())
   if err != nil {
     log.Fatalf("did not connect: %s", err)
   }
   opcion:=""
   defer conn.Close()   
   fmt.Scanf("%s", &opcion)
   c := pb.NewGreeterClient(conn)
   response, err := c.SolicitarUbicaciones(context.Background(), &pb.ConsultaUbicacion{NombreArchivo:opcion})
   if err != nil {
     log.Fatalf("Error when calling SayHello: %s", err)
   }
	 partes:=response.Partes
	 ubicacion:=response.Ubicaciones
	 return int(partes),ubicacion,opcion
}


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

func sendChunk(partToSend int, bookName string) {
	chunkToSend := bookName + "_" + strconv.FormatUint(uint64(partToSend), 10)
	chunkBytes, err := ioutil.ReadFile(chunkToSend) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}
	// Ver a donde enviar los chunkbytes probar hasta que algun data node responda
	// por defecto pruebo con el 158
	maquina:="dist157"
	var conn *grpc.ClientConn	
	conn, err := grpc.Dial(maquina+":50054", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()	
	c := pb.NewGreeterClient(conn)	
	response, err := c.SayHello(context.Background(), &pb.DataChuck{Valor: int32(partToSend), Chunck: chunkBytes})
	if err != nil {
		log.Fatalf("Error when calling SayHello: %s", err)
	}
	fmt.Println("# DataNode responde: Se a recibido chunk numero ",responde.Valor )
	return 
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


/*-----------------------------------------------------------------------------------------*/
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

//func archivos_disponibles() string[]{
  // DEBERIA RETORNAR LA LISTA DE ARCHIVOS DISPONIBLES
//}

//func verificar_archivo( nombre_archivo string) int{
  // DEBERIA verificar si el archivo existe o no
  // 0 si existe 1 si no
//}

func solicitar_archivo(){
	//archivos_dis:=archivos_disponibles()   
	opcion:="bandera"
	check:=0
	archivos_dis:=["test.pdf"]	
	for check==0{ 	
		fmt.Println("### Los archivos disponibles son los siguientes:")
		for i := 0; i < len(archivos_dis); i++ {
			fmt.Println(archivos_dis[i])		
		}	
	    fmt.Println("Ingrese el nombre exacto de alguno de los archivos disponibles")	    
	    fmt.Scanf("%s", &opcion)
	    //check=verificar_archivo(opcion)
	    if check==0{
	    	partes,maquinas,nameFile:=pedir_archivo(opcion)
	    	//chequear las maquinas
	    	aux_maquina:=strings.Split(maquinas, "-")
			totalChunks:=uint64(partes)
			aux:=0
			for j := uint64(0); j < totalChunks; j++ {
				aux=int(j)
				requestChunk(aux_maquina[aux],aux,nameFile)
			}
			stitchTheFile(nameFile, totalChunks)
			fmt.Println("[°] Archivo Reconstruido y disponible")

	    }else{
	    	fmt.Println("### Escriba un nombre de archivo valido")
	    }	
	}
}

func subir_archivo(){
	fmt.Println("# Ingrese el nombre exacto del archivo que va a subir")	
	fmt.Println("# Ejemplo: test.pdf ")	
	fmt.Scanf("%s", &opcion)
	/// valor si existe el archivo
	fmt.Println("[°] Comenzando proceso para subir el archivo")	
	cantidad_partes:=gutTheFile(opcion)
	for i := 0; i < cantidad_partes; i++ {
		sendChunk(i, opcion)		
	}
	fmt.Println("[°] Todos los chunks enviados")	
}	

func menu(){
	opcion:=0	
	fmt.Println("### Bienvenid@ a la tarea 2 de Distribuidos")
	for opcion!=3{
		fmt.Println("Escriba 1 para poder subir un pdf")
		fmt.Println("Escriba 2 para poder pedir un pdf")
		fmt.Println("Escriba 3 para salir")
		fmt.Scanf("%d", &opcion)
		if opcion==1{
			subir_archivo()			
		}
		if opcion==2{
			solicitar_archivo()
		}		
	}
	
}

func main() {
	menu()	
}
