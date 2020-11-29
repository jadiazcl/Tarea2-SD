package main

 import (
         "fmt"
         "log"
         "os"
         "strings"
         "strconv"
         "net"         
         "bufio"
         "google.golang.org/grpc"
         "context"
          pb"Lab2-Test/Tarea2-SD/pipeline"
 )

 var Maquinas = map[string]int{"dist157": 0, "dist158": 1,"dist159": 2,"dist160": 3}


 type Server struct {
    pb.UnimplementedGreeterServer
 }

 //La funcion GRPC para la consulta de la ubicacion del archivo
 func (s *Server) SolicitarUbicaciones(ctx context.Context, in *pb.ConsultaUbicacion) (*pb.RespuestaUbicacion, error) {
   log.Printf("recibi %s ", in.NombreArchivo )
   partes,ubicaciones:=buscar_en_log(in.NombreArchivo)
 	return &pb.RespuestaUbicacion{Partes: int32(partes),Ubicaciones:ubicaciones}, nil
 }

 // Esta funcion busca la ubicacion de las partes del archivo
 func buscar_en_log(nombre_libro string) (int, string){
    file, err := os.Open("log.txt")
    if err != nil {
        log.Fatalf("Error when opening file: %s", err)
    }
    fileScanner := bufio.NewScanner(file)
    ubicacion:=""
    cantidad_saltos:=0
    for fileScanner.Scan() {
      if (cantidad_saltos==0){
        linea:=fileScanner.Text()
        partes:=strings.Split(linea, " ")
        nombre_registro:=partes[0]
        cantidad_saltos,err=strconv.Atoi(partes[1])
        if (nombre_registro==nombre_libro){
          for index := 0; index < cantidad_saltos; index++ {
            fileScanner.Scan()
            linea:=fileScanner.Text()
            partes:=strings.Split(linea, " ")
            ubicacion=ubicacion+partes[1]+"-"
          }
          break
        }
      }else{
        cantidad_saltos=cantidad_saltos-1
      }
    }
    return cantidad_saltos,ubicacion
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
