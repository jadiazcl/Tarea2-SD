package main

 import (
         "fmt"
         "log"
         "net"
         "google.golang.org/grpc"
         "context"
          pb"Lab2-Test/Tarea2-SD/pipeline"
 )
 // Maquina{ 1: 158 ; 2 : 159 ; 3 : 160}


 type Server struct {
     pb.UnimplementedGreeterServer
 }

 //La funcion GRPC para la consulta de la ubicacion del archivo
 func (s *Server) SolicitarUbicaciones(ctx context.Context, in *pb.ConsultaUbicacion) (*pb.RespuestaUbicacion, error) {
    log.Printf("recibi %d ", in.IdArchivo )
    partes,ubicaciones:=ubicacion_archivo(int(in.IdArchivo))
 	return &pb.RespuestaUbicacion{Partes: int32(partes),Ubicaciones:ubicaciones}, nil
 }

 // Esta funcion busca la ubicacion de las partes del archivo
 func ubicacion_archivo(id_archivo int) (int, string){
    // Codigo para buscar el archivo
    ubicacion:="1-1-1-1-1"
    return 5,ubicacion

 }

 //funcion para recepcionar conexiones

 func  recepcion_clientes(){
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

 func main() {
    go recepcion_clientes()
    opcion:=0
    for opcion!=-1{
        fmt.Println("Ingrese -1 para cerrar el programa ")
        fmt.Scanf("%d", &opcion)
    }
  }