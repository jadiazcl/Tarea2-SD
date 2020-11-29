package main

import (
  "log"
  "fmt"
  //"io/ioutil"
  //"os"
  //"strconv"
  "golang.org/x/net/context"
  "google.golang.org/grpc"
  pb"Lab2-Test/Tarea2-SD/pipeline"
)



// Maquina{ 1: 158 ; 2 : 159 ; 3 : 160}

func pedir_archivo(){
   var conn *grpc.ClientConn
   conn, err := grpc.Dial("dist157:50054", grpc.WithInsecure())
   if err != nil {
     log.Fatalf("did not connect: %s", err)
   }
   opcion:=0
   defer conn.Close()
   for opcion!=-1{
       fmt.Println("Ingrese -1 para cerrar el programa ")
       fmt.Scanf("%d", &opcion)
     c := pb.NewGreeterClient(conn)
     response, err := c.SolicitarUbicaciones(context.Background(), &pb.ConsultaUbicacion{NombreArchivo:"test.pdf"})
     if err != nil {
       log.Fatalf("Error when calling SayHello: %s", err)
     }
     log.Printf("El codigo de seguimiento del pedido es: %d", response.Partes)
     log.Printf("Ubicacion: %s", response.Ubicaciones)
   }
}

func main() {
	pedir_archivo()
}
