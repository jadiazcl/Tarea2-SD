package main

import (
	pb "Lab2/Tarea2-SD/pipeline"
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
)

const (
	address = "dist158:50054"
)

func enviar_ordenes(delta_tiempo float64) {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist160:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)
	i := 0
	updateTime := time.Now()
	time2 := time.Now()
	for i < 10 {
		time2 = time.Now()
		if time2.Sub(updateTime).Seconds() > delta_tiempo {
			response, err := c.SayHello(context.Background(), &pb.ConsultaEstado{IdCamion: i})
			if err != nil {
				log.Fatalf("error %s", err)
			}
			log.Printf("respuesta:  %d", response.Seguimiento)
			i++
			updateTime = time.Now()
		}
	}
}

func main() {
	enviar_ordenes(delta_tiempo)

}
