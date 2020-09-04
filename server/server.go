package main

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo"
	"os/signal"
	"os"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	pb "github.com/ealecho/mongoblog/blogpb"
	"context"
	"time"
	"google.golang.org/grpc/reflection"
)


var collection *mongo.Collection

type server struct{ }

type blogItem struct {
	ID string 				`bson:"_id,omitempty"`
	AuthorID string 		`bson:"author_id"`
	Content string 			`bson:"content"`
	Title string 			`bson:"title"`
}



func (*server) CreateBlog(ctx context.Context, req *pb.CreateBlogRequest) (*pb.CreateBlogResponse, error) {
	fmt.Printf("Create Blog request: %v", req)
	blog := req.GetBlog()

	data := blogItem{
		AuthorID: blog.GetAuthorId(), 		
		Content: blog.GetContent(),		
		Title:blog.GetTitle(),
	}
	
	//Force MongoDB to always set its own generated ObjectID
	data.ID = ""
	
	// insert the record
	insertionResult, err := collection.InsertOne(context.Background(), data)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Internal error: %v", err),
		)
	}

	//get the just inserted record in order to return its a response
	filter := bson.D{{Key: "_id", Value: insertionResult.InsertedID}}
	createdBlogRecord := collection.FindOne(context.Background(), filter)

	createdBlog := &blogItem{}
	createdBlogRecord.Decode(createdBlog)

	return &pb.CreateBlogResponse{
		Blog: &pb.Blog{
			Id: createdBlog.ID,
			AuthorId: createdBlog.AuthorID, 		
			Content: createdBlog.Content,		
			Title:createdBlog.Title,
		},
	},nil
}

func (*server) ReadBlog(ctx context.Context, req *pb.ReadBlogRequest) (*pb.ReadBlogResponse, error) {
	fmt.Printf("Create Blog request: %v\n", req)
	blogID := req.GetBlogId()

	//convert id string to object id
	objectID, err := primitive.ObjectIDFromHex(blogID)
	if err != nil {

		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Unable to convert id string to ObjectId %v", err),
		)
		
	}
	filter := bson.D{{Key: "_id", Value: objectID}}
	blogRecord := collection.FindOne(context.Background(), filter)

	foundBlog := &blogItem{}
	blogRecord.Decode(foundBlog)

	return &pb.ReadBlogResponse{
		Blog: &pb.Blog{
			Id: foundBlog.ID,
			AuthorId: foundBlog.AuthorID, 		
			Content: foundBlog.Content,		
			Title:foundBlog.Title,
		},
	},nil


}

func (*server) UpdateBlog(ctx context.Context, req *pb.UpdateBlogRequest) (*pb.UpdateBlogResponse, error) {
	log.SetFlags(log.LstdFlags | log.Lshortfile) 
	fmt.Printf("Update Blog request: %v\n", req)
	blog := req.GetBlog()

	// update := &blogItem{
	// 	AuthorID: blog.GetAuthorId(), 		
	// 	Content: blog.GetContent(),		
	// 	Title:blog.GetTitle(),
	// }

	update := bson.D{{
		Key: "$set",
		Value:bson.D{
			{Key: "author_id", Value: blog.GetAuthorId()},
			{Key: "content", Value: blog.GetContent()},
			{Key: "title", Value: blog.GetTitle()},
		},
	}}

	//fmt.Println(update)
	objectID, err := primitive.ObjectIDFromHex(blog.GetId())
	if err != nil {

		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Unable to convert id string to ObjectId %v", err),
		)
		
	}
	filter := bson.D{{Key: "_id", Value: objectID}}

	upsert := true
	after := options.After

	//Create an instance of an options and set the desired options
	opt := options.FindOneAndUpdateOptions{
		ReturnDocument: &after,
		Upsert:         &upsert,
	}

	updatedBlogRecord := collection.FindOneAndUpdate(context.Background(), filter, update, &opt)

	updatedBlog := &blogItem{}
	updatedBlogRecord.Decode(updatedBlog)

	return &pb.UpdateBlogResponse{
		Blog: &pb.Blog{
			Id: updatedBlog.ID,
			AuthorId: updatedBlog.AuthorID, 		
			Content: updatedBlog.Content,		
			Title:updatedBlog.Title,
		},
	},nil

}

func (*server) DeleteBlog(ctx context.Context, req *pb.DeleteBlogRequest) (*pb.DeleteBlogResponse, error) {
	fmt.Printf("Delete Blog request: %v\n", req)
	blogID := req.GetBlogId()

	//convert id string to object id
	objectID, err := primitive.ObjectIDFromHex(blogID)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Unable to convert id string to ObjectId %v", err),
		)
		
	}
	filter := bson.D{{Key: "_id", Value: objectID}}
	blogRecord := collection.FindOneAndDelete(context.Background(), filter)

	deletedBlog := &blogItem{}
	blogRecord.Decode(deletedBlog)

	return &pb.DeleteBlogResponse{
		BlogId:deletedBlog.ID,	
	},nil
}

func (*server) ListBlog(req *pb.ListBlogRequest, stream pb.BlogService_ListBlogServer) error {
	fmt.Printf("List Blog request: %v\n", req)
	

	filter := bson.D{{}}
	cursor, err := collection.Find(context.Background(), filter)

	if err != nil {
	 status.Errorf(
			codes.Internal,
			fmt.Sprintf("Trouble finding Blog %v", err),
		)
	}

	
	var Blogs []blogItem = make([]blogItem, 0)

	// iterate the cursor and decode each item into an Employee
	if err := cursor.All(context.Background(), &Blogs); err != nil {
		status.Errorf(
			codes.Internal,
			fmt.Sprintf("Trouble finding Blog %v", err),
		)
	}

	for _, blog := range Blogs {

		res := &pb.ListBlogResponse{
			Blog:&pb.Blog{
				Id: blog.ID,
				AuthorId: blog.AuthorID, 		
				Content: blog.Content,		
				Title:blog.Title,
			},
		}

		stream.Send(res)
		
	}
	return nil
}


func main() {
	//if we crash the go code, we get to know where the proble was
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	fmt.Println("Blog service has Started")
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen %v", err)
	}

	//connet to mongodb
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil { log.Fatal(err) }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil { log.Fatal(err) }


	collection = client.Database("myDb").Collection("blog")

	opts := []grpc.ServerOption{}
	s := grpc.NewServer(opts...)
	pb.RegisterBlogServiceServer(s, &server{})
	
	//enable server reflection
	reflection.Register(s)
	go func(){
		fmt.Println("Starting server....")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	//Wait for control c to exit
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	
	<-ch
	// Second step : closing the listener
	fmt.Println("Closing the listener")
	if err := lis.Close(); err != nil {
		log.Fatalf("Error on closing the listener : %v", err)
	}

	// Finally, we stop the server

	fmt.Println("Stopping the server")
	s.Stop()
	fmt.Println("Disconnecting from MongoDB")
	client.Disconnect(ctx)
	fmt.Println("End of Program")
	


}