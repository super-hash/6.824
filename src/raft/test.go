package main

import "fmt"

func main() {

	Tasks := make(chan int, 1000)
	for i := 0; i < 10; i++ {
		Tasks <- i
	}
	fmt.Printf("%v\n",len(Tasks))
	// for i:=0;i<10;i++{
	// 	a:=<-Tasks
	// 	// if a==3{
	// 	// 	fmt.Println("have 3")
	// 	// }else{
	// 	// 	Tasks<-a
	// 	// }
	// }
	for i := 0; i < len(Tasks); i++ {
		a := <-Tasks
		fmt.Printf("%v", a)
	}
	fmt.Println()
	fmt.Printf("%v",len(Tasks))
	
}
