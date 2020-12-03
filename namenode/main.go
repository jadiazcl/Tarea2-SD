package main

import (
	pb "Lab2-Centralizada/Tarea2-SD/pipeline"
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

var Maquinas = map[string]int{"dist157": 0, "dist158": 1, "dist159": 2, "dist160": 3}

type Server struct {
	pb.UnimplementedGreeterServer
}

//La funcion GRPC para la consulta de la ubicacion del archivo
func (s *Server) SolicitarUbicaciones(ctx context.Context, in *pb.ConsultaUbicacion) (*pb.RespuestaUbicacion, error) {
	log.Printf("recibi %s ", in.NombreArchivo)
	partes, ubicaciones := buscar_en_log(in.NombreArchivo)
	return &pb.RespuestaUbicacion{Partes: int32(partes), Ubicaciones: ubicaciones}, nil
}

// conecta con cliente para entregar archivos Disp

func (s *Server) FilesAvl(ctx context.Context, in *pb.Resultado) (*pb.ConsultaUbicacion, error) {
	stringArchivos := FilesOnLog()
	return &pb.ConsultaUbicacion{NombreArchivo: stringArchivos}, nil
}

// Esta funcion busca los archivos en LOG
func FilesOnLog() string {
	XFiles := "" // archivos en LOG
	file, err := os.Open("log.txt")
	if err != nil {
		log.Fatalf("Error when opening file: %s", err)
	}
	fileScanner := bufio.NewScanner(file)
	cantidad_saltos := 0
	for fileScanner.Scan() {
		linea := fileScanner.Text()
		partes := strings.Split(linea, " ")
		cantidad_saltos, err = strconv.Atoi(partes[1])

		nombre_registro := partes[0]
		XFiles = XFiles + "-" + nombre_registro
		for index := 0; index < cantidad_saltos; index++ {
			fileScanner.Scan()
		}
	}

	return XFiles
}

func escribir_log(distribucion string, nombre_libro string) {
	maquinas := strings.Split(distribucion, "-")
	theLog, err := os.Create("log.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer theLog.Close()
	parts := len(maquinas)
	strParts := strconv.Itoa(parts)
	aux_string := nombre_libro + strParts + "\n"
	_, err2 := theLog.WriteString(aux_string)
	if err2 != nil {
		log.Fatal(err2)
	}
	aux := ""
	for i := 0; i < parts; i++ {
		a := i + 1
		aux = strconv.Itoa(a)
		aux_string = "parte_" + aux + " " + maquinas[i]
		_, err3 := theLog.WriteString(aux_string)
		if err2 != nil {
			log.Fatal(err3)
		}

	}

}

// Esta funcion busca la ubicacion de las partes del archivo
func buscar_en_log(nombre_libro string) (int, string) {
	file, err := os.Open("log.txt")
	if err != nil {
		log.Fatalf("Error when opening file: %s", err)
	}
	fileScanner := bufio.NewScanner(file)
	ubicacion := ""
	cantidad_saltos := 0
	for fileScanner.Scan() {
		if cantidad_saltos == 0 {
			linea := fileScanner.Text()
			partes := strings.Split(linea, " ")
			nombre_registro := partes[0]
			cantidad_saltos, err = strconv.Atoi(partes[1])
			if nombre_registro == nombre_libro {
				for index := 0; index < cantidad_saltos; index++ {
					fileScanner.Scan()
					linea := fileScanner.Text()
					partes := strings.Split(linea, " ")
					ubicacion = ubicacion + partes[1] + "-"
				}
				break
			}
		} else {
			cantidad_saltos = cantidad_saltos - 1
		}
	}
	return cantidad_saltos, ubicacion
}

//funcion para recepcionar conexiones
func recepcion_clientes() {
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
	opcion := 0
	for opcion != -1 {
		fmt.Println("Ingrese -1 para cerrar el programa ")
		fmt.Scanf("%d", &opcion)
	}
	escribir_log("m1-m2-m3-m4-m5", "file")
}
