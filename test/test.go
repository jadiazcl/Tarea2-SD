package main

 import (
         "bufio"
         "fmt"
         "io/ioutil"
         "math"
         "os"
         "log"
         "net"
         "strconv"
         "google.golang.org/grpc"
         "context"
          pb"Lab2/Tarea2-SD/pipeline"
 )

 type Server struct {
     pb.UnimplementedGreeterServer
 }

 func (s *Server) SayHello(ctx context.Context, in *pb.Solcamion) (*pb.Test, error) {
 	log.Printf("recibi %d ", in.IdCamion )
  auxiliar:=test_archivo()
 	return &pb.Test{Valor: in.IdCamion,Chuck_data:auxiliar}, nil
 }

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

 func test_archivo() []byte{
   fileToBeChunked := "test.pdf" // change here!

   file, err := os.Open(fileToBeChunked)

   if err != nil {
           fmt.Println(err)
           os.Exit(1)
   }

   defer file.Close()

   fileInfo, _ := file.Stat()

   var fileSize int64 = fileInfo.Size()

   const fileChunk = 256000 // 1 MB, change this to your requirement
   // calculate total number of parts the file will be chunked into

   totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

   fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

   for i := uint64(0); i < totalPartsNum; i++ {

           partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
           partBuffer := make([]byte, partSize)
           break
   }
   return partBuffer
   // just for fun, let's recombine back the chunked files in a new file
 }




 func main() {
         go recepcion_clientes()
         test_archivo()
         opcion:=0
         for opcion!=-1{
             fmt.Println("Ingrese -1 para cerrar el programa ")
             fmt.Scanf("%d", &opcion)
         }
 }
