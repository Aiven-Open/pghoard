/*
pghoard - archive_command and restore_command for postgresql

Copyright (c) 2017 Aiven
See LICENSE for details
*/

package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"time"
)

const PGHOARD_HOST = "127.0.0.1"
const PGHOARD_PORT = 16000

// When running restore_command PostgreSQL interprets exit codes 1..125 as "file not found errors" signalling
// that there's no such WAL file from which PostgreSQL assumes that we've completed recovery.  We never want to
// return such an error code unless we actually got confirmation that the requested file isn't in the backend so
// we try to exit with EXIT_ERROR (255) status whenever we see unexpected errors.  Such an error code causes
// PostgreSQL to abort recovery and wait for admin interaction.
//
// The above considerations apply to handling archive_command, but in its case there's no reason for us to ask
// PostgreSQL to abort, we want it to just retry indefinitely so we'll always return a code between 1..125.
//
// Note that EXIT_NOT_FOUND and EXIT_ARCHIVE_FAIL and their error codes are not defined or required by
// PostgreSQL, they're just used for convenience here and to test for differences between various failure
// scenarios (Python exits with status 1 on uncaught exceptions.)
const EXIT_OK = 0
const EXIT_RESTORE_FAIL = 2
const EXIT_ARCHIVE_FAIL = 3
const EXIT_NOT_FOUND = 4
const EXIT_ABORT = 255

func main() {
	rc, err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	os.Exit(rc)
}

func run() (int, error) {
	verPtr := flag.Bool("version", false, "show program version")
	hostPtr := flag.String("host", PGHOARD_HOST, "pghoard service host")
	portPtr := flag.Int("port", PGHOARD_PORT, "pghoard service port")
	sitePtr := flag.String("site", "", "pghoard backup site")
	xlogPtr := flag.String("xlog", "", "xlog file name")
	outputPtr := flag.String("output", "", "output file")
	modePtr := flag.String("mode", "", "operation mode")
	riPtr := flag.Float64("retry-interval", 5.0, "retry interval (seconds)")

	flag.Parse()

	if *verPtr {
		fmt.Println("pghoard_postgres_command_go 1.0.0")
		return EXIT_OK, nil
	}

	if *sitePtr == "" {
		return EXIT_ABORT, errors.New("--site flag is required")
	}
	if *xlogPtr == "" {
		return EXIT_ABORT, errors.New("--xlog flag is required")
	}

	url := fmt.Sprint("http://", *hostPtr, ":", *portPtr, "/", *sitePtr, "/archive/", *xlogPtr)

	if *modePtr == "archive" {
		return archive_command(url)
	} else if *modePtr == "restore" {
		attempt := 0
		retry_seconds := *riPtr
		for {
			attempt += 1
			rc, err := restore_command(url, *outputPtr)
			if rc != EXIT_RESTORE_FAIL {
				return rc, err
			}
			if attempt >= 3 {
				return EXIT_ABORT, err // see the comment at the top of this file
			}
			log.Printf("Restoring %s failed: %s; retrying in %g seconds", *xlogPtr, err, retry_seconds)
			time.Sleep(time.Duration(retry_seconds) * time.Second)
		}
	} else {
		return EXIT_ABORT, errors.New("--mode must be set to 'archive' or 'restore'")
	}
}

func archive_command(url string) (int, error) {
	return EXIT_ABORT, errors.New("archive_command not yet implemented")
}

func restore_command(url string, output string) (int, error) {
	var output_path string
	var req *http.Request
	var err error

	if output == "" {
		req, err = http.NewRequest("HEAD", url, nil)
	} else {
		/* Construct absolute path for output - postgres calls this command with a relative path to its xlog
		directory.  Note that os.path.join strips preceding components if a new components starts with a
		slash so it's still possible to use this with absolute paths. */
		if output[0] == '/' {
			output_path = output
		} else {
			cwd, err := os.Getwd()
			if err != nil {
				return EXIT_ABORT, err
			}
			output_path = path.Join(cwd, output)
		}
		req, err = http.NewRequest("GET", url, nil)
		req.Header.Set("x-pghoard-target-path", output_path)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return EXIT_RESTORE_FAIL, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return EXIT_RESTORE_FAIL, err
	}

	// no output requested, expecting 200 OK for a HEAD request
	if output == "" && len(body) == 0 && resp.StatusCode == 200 {
		return EXIT_OK, nil
	}

	// output requested, expecting 201 Created response
	if output != "" && len(body) == 0 && resp.StatusCode == 201 {
		return EXIT_OK, nil
	}

	/* NOTE: PostgreSQL interprets exit codes 1..125 as "file not found errors" signalling that there's no
	such wal file from which PostgreSQL assumes that we've completed recovery so we never want to return
	such an error code unless we actually got confirmation that the file isn't in the backend. */
	if resp.StatusCode == 404 {
		log.Printf("%s not found from archive", url)
		return EXIT_NOT_FOUND, nil
	}
	return EXIT_ABORT, fmt.Errorf("Restore failed with HTTP status %d: %s", resp.StatusCode, string(body))
}
