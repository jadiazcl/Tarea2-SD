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

// maquinas=["158,159,160"]

//func enviar_ordenes( id_maquina  int, parte int){
func enviar_ordenes(){
  var conn *grpc.ClientConn
  conn, err := grpc.Dial("dist160:50054", grpc.WithInsecure())
  if err != nil {
    log.Fatalf("did not connect: %s", err)
  }
  opcion:=0
  defer conn.Close()
  for opcion!=-1{
      fmt.Println("Ingrese -1 para cerrar el programa ")
      fmt.Scanf("%d", &opcion)
    c := pb.NewGreeterClient(conn)
    response, err := c.SayHello(context.Background(), &pb.Solcamion{IdCamion:int32(opcion)})
    if err != nil {
      log.Fatalf("Error when calling SayHello: %s", err)
    }
    log.Printf("El codigo de seguimiento del pedido es: %d", response.Valor)
    //fileName := "bigfile_" + strconv.FormatUint(parte, 10)
    fileName := "bigfile_" + strconv.FormatUint(1, 10)
    ioutil.WriteFile(fileName, response.Chuck, os.ModeAppend)
  }
}

// func recuperar_archivo(){
//     enviar_ordenes( 0,0)
//     enviar_ordenes( 0,1)
//     enviar_ordenes( 1,2)
//     enviar_ordenes( 2,3)
//
//
//   //rearmar objeto
// }


func main() {
	enviar_ordenes()

}
