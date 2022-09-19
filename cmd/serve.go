package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/service/rest"
	"os"
	"os/signal"
	"syscall"
)

var (
	port int
)

var serveCmd = &cobra.Command{
	Use:   "serve [command]",
	Short: "run the dataflow server",
	Run: func(cmd *cobra.Command, args []string) {
		go rest.NewRestService().Serve(port)
		gracefulStop()
	},
}

func gracefulStop() {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	fmt.Printf("receive signal %s \n", sig)
	fmt.Println("Graceful Exit")
	os.Exit(0)
}

func init() {
	serveCmd.PersistentFlags().StringVar(&infra.MongoURI, "mongo", "mongodb://root:123456@localhost:27017", "mongoDB URI(default:`mongodb://root:123456@localhost:27017`)")
	serveCmd.PersistentFlags().IntVarP(&port, "port", "p", 8080, "dataflow server export http port")
}
