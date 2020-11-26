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

// func requestChunk(){
//   var conn *grpc.ClientConn
//   conn, err := grpc.Dial("dist160:50054", grpc.WithInsecure())
//   if err != nil {
//     log.Fatalf("did not connect: %s", err)
//   }
//   opcion:=0
//   defer conn.Close()
//   for opcion!=-1{
//       fmt.Println("Ingrese -1 para cerrar el programa ")
//       fmt.Scanf("%d", &opcion)
//     c := pb.NewGreeterClient(conn)
//     response, err := c.SayHello(context.Background(), &pb.Solcamion{IdCamion:int32(opcion)})
//     if err != nil {
//       log.Fatalf("Error when calling SayHello: %s", err)
//     }
//     log.Printf("El codigo de seguimiento del pedido es: %d", response.Valor)
//     //fileName := "bigfile_" + strconv.FormatUint(parte, 10)
//     fileName := "bigfile_" + strconv.FormatUint(1, 10)
//     ioutil.WriteFile(fileName, response.Chuck, os.ModeAppend)
//   }
// }

//func requestChunk( idMchn  int, parte int){
func requestChunk(idMchn int) {

	machines := []string{"dist157", "dist158", "dist159", "dist160"}
	var conn *grpc.ClientConn
	mchn := machines[idMchn]
	conn, err := grpc.Dial(mchn+":50054", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	opcion := 0
	defer conn.Close()
	for opcion != -1 {
		fmt.Println("waiting >>>")
		fmt.Scanf("%d", &opcion)
		c := pb.NewGreeterClient(conn)
		// response, err := c.SayHello(context.Background(), &pb.Test{Valor:int32(parte), Chuck:------}
		response, err := c.SayHello(context.Background(), &pb.Solcamion{IdCamion: int32(opcion)})

		if err != nil {
			log.Fatalf("Error when calling SayHello: %s", err)
		}
		log.Printf("El codigo de seguimiento del pedido es: %d", response.Valor)
		fileName := "bigfile_" + strconv.FormatUint(0, 10)
		// fileName := "bigfile_" + strconv.FormatUint(opcion, 10)
		ioutil.WriteFile(fileName, response.Chuck, os.ModeAppend)
	}
}

// func recuperar_archivo(){
//     requestChunk( 0,0)
//     requestChunk( 0,1)
//     requestChunk( 1,2)
//     requestChunk( 2,3)
//
//
//   //rearmar objeto
// }

func main() {
	requestChunk(3)

}
