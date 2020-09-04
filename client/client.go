package main

import (
	"fmt"
	"log"
	"google.golang.org/grpc"
	pb "github.com/ealecho/mongoblog/blogpb"
	"context"
	"io"
)




func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	fmt.Println("Establishing connection on TCP :50051")
	opts := grpc.WithInsecure()
	cc ,err := grpc.Dial(":50051", opts)
	defer cc.Close()

	if err != nil{
		log.Fatalf("Could not establish connection on TCP :50051: %v", err)
	}

	c := pb.NewBlogServiceClient(cc)



	//createBlog
	createBlogreq := &pb.CreateBlogRequest{
		Blog: &pb.Blog{
			AuthorId:"Gringo M",
			Title:"In Mehico",
			Content: "Liko alien in mehico.",
		},
	}

	createBlogres, err := c.CreateBlog(context.Background(), createBlogreq)
	if err != nil {
		log.Fatalf("error while calling rpc: %v", err)
	}
	log.Printf("Response from createBlog: %v", createBlogres.Blog)


	//ReadBlog
	readBlogreq := &pb.ReadBlogRequest{
		BlogId:"5f50ffe68b44a9caf8362e78",
	}
	
	readBlogres, err := c.ReadBlog(context.Background(), readBlogreq)
	if err != nil {
		log.Fatalf("error while calling rpc: %v", err)
	}
	log.Printf("Response from readBlog: %v", readBlogres.Blog)


	//updateBlog
	updateBlogreq := &pb.UpdateBlogRequest{
		Blog: &pb.Blog{
			Id:"5f50fccbe68e8c64dd854e62",
			AuthorId:"Miles Mabiike",
			Title:"Why are you running",
			Content: "He run run",
		},
	}
	
	updateBlogres, err := c.UpdateBlog(context.Background(), updateBlogreq)
	if err != nil {
		log.Fatalf("error while calling rpc: %v", err)
	}
	log.Printf("Response from updateBlog: %v", updateBlogres.Blog)


	//deleteBlogCall
	deleteReq := &pb.DeleteBlogRequest{
		BlogId:"5f50fccbe68e8c64dd854e62",
	}
	
	deleteReqres, err := c.DeleteBlog(context.Background(), deleteReq)
	if err != nil {
		log.Fatalf("error while calling rpc: %v", err)
	}
	log.Printf("Response from deleteBlog: %v", deleteReqres.BlogId)


	//listBlogCall
	reStream, err := c.ListBlog(context.Background(), &pb.ListBlogRequest{})
	if err != nil {
		log.Fatalf("error while calling rpc: %v", err)
	}
	
	for {
		msg, err := reStream.Recv()
		if err == io.EOF {
			// we've reached the end of the stream
			break
		}
		if err != nil {
			log.Fatalf("error while reading stream: %v", err)
		}
		log.Printf("Response from listBlog: %v", msg.GetBlog())
	}

	

}