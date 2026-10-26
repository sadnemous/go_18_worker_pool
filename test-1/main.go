package main

import (
	"fmt"
	"sync"
	"time"
)

func worker(
	id int,
	jobs <-chan int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Keep receiving jobs until the channel is closed.
	for job := range jobs {
		fmt.Printf("Worker %d started job %d\n", id, job)

		time.Sleep(2 * time.Second)

		fmt.Printf("==Worker %d finished job %d\n", id, job)
	}

	fmt.Printf("Worker %d exiting\n", id)
}

func main() {
	start := time.Now()

	const numWorkers = 2
	const numJobs = 10

	jobs := make(chan int) // Unbuffered channel.

	var wg sync.WaitGroup

	// Launch two workers.
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, &wg)
	}

	// Send ten jobs through the same channel.
	for j := 1; j <= numJobs; j++ {
		jobs <- j
	}
	fmt.Printf("Closing Channel\n")
	close(jobs)
	wg.Wait()

	fmt.Println("Total:", time.Since(start))
}
