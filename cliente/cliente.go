package main

import (
  "os"
  "log"
	"encoding/csv"
	"fmt"
	"io"
  "strconv"
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
  defer conn.Close()
  c := pb.NewGreeterClient(conn)
  i:=0
  update_time:=time.Now()
  time2:=time.Now()
  for  i < len(ordenes){
    time2=time.Now()
    if ( time2.Sub(update_time).Seconds() > delta_tiempo){
      response, err := c.SayHello(context.Background(), &pb.Solcamion{IdCamion:1})
      if err != nil {
        log.Fatalf("Error when calling SayHello: %s", err)
      }
      log.Printf("El codigo de seguimiento del pedido es: %d", response.IdCamion)
      i=i+1
      update_time=time.Now()
    }
  }
}
func main() {
	enviar_ordenes(delta_tiempo)

}
