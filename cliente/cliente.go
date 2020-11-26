package main

import (
  "log"
  "time"
  "golang.org/x/net/context"
  "google.golang.org/grpc"
    pb"Lab2/Tarea2-SD/pipeline"
)


func enviar_ordenes( delta_tiempo float64){
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
    response, err := c.SayHello(context.Background(), &pb.Solcamion{IdCamion:1})
    if err != nil {
      log.Fatalf("Error when calling SayHello: %s", err)
    }
    log.Printf("El codigo de seguimiento del pedido es: %d", response.IdCamion)
  }
}
func main() {
	enviar_ordenes(2)

}
