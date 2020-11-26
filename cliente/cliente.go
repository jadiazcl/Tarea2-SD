package main

import (
  "log"
  "fmt"
  "io/ioutil"
  "os"
  "strconv"
  "golang.org/x/net/context"
  "google.golang.org/grpc"
  pb"Lab2/Tarea2-SD/pipeline"
)

// func request_chunk(){
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


var machines string[] = ["dist158", "dist159", "dist160"]


func request_chunk( id_maquina  int, parte int){
  var conn *grpc.ClientConn
  mchn := 
  conn, err := grpc.Dial("dist160:50054", grpc.WithInsecure())
  if err != nil {
    log.Fatalf("did not connect: %s", err)
  }
  opcion:=0
  defer conn.Close()
  for opcion!=-1{
      fmt.Println("waiting >>>")
      fmt.Scanf("%d", &opcion)
    c := pb.NewGreeterClient(conn)
    response, err := c.SayHello(context.Background(), &pb.Test{Valor:int32(parte), Chuck:------})
    if err != nil {
      log.Fatalf("Error when calling SayHello: %s", err)
    }
    log.Printf("El codigo de seguimiento del pedido es: %d", response.Valor)
    //fileName := "bigfile_" + strconv.FormatUint(parte, 10)
    fileName := "bigfile_" + strconv.FormatUint(opcion, 10)
    ioutil.WriteFile(fileName, response.Chuck, os.ModeAppend)
  }
}
// func recuperar_archivo(){
//     request_chunk( 0,0)
//     request_chunk( 0,1)
//     request_chunk( 1,2)
//     request_chunk( 2,3)
//
//
//   //rearmar objeto
// }


func main() {
	request_chunk()

}
