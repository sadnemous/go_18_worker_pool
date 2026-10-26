package main

import (
	"fmt"
	"time"
	"sync"
)

type Job struct {
	ID int
	Input int
}

type Result struct {
	JobID int
	WorkerID int
	Output int
}

func worker(ID int, jobs <- chan Job, result chan <- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		time.Sleep(200 * time.Millisecond)
		result <- Result {
			JobID: job.ID,
			WorkerID: ID,
			Output: job.Input * 10,
		}
	}
}


func main() {
	const numOfJobs = 10
	const numOfWorkers = 3

	jobs := make(chan Job, numOfJobs)
	results := make(chan Result, numOfJobs)
	var wg sync.WaitGroup

	// start the workers
	for w:=1; w<=numOfWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, results, &wg)
	}
	// submit the jobs
	for j:=1; j<=numOfJobs; j++ {
		jobs <- Job {
			ID: j,
			Input: j * 2,
		}
	}

	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	// read the results
	for res := range results {
		fmt.Printf("WorkerID: %d JobID: %d Output: %d\n", res.WorkerID, res.JobID, res.Output)
	}
}




		

