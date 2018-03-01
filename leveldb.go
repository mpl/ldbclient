/*
Copyright 2018 The Perkeep Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"log"

	"perkeep.org/pkg/sorted/leveldb"
)

func main() {
	flag.Parse()

	args := flag.Args()
	largs := len(args)
	if largs < 2 || largs > 3 {
		log.Fatalf("want 2 or 3 args, not %d", largs)
	}

	var err error
	switch args[1] {
	case "ls":
		err = list(args[0])
	case "rm":
		err = delete(args[0], args[2])
	default:
		log.Fatal("unknown command")
	}
	if err != nil {
		log.Fatal(err)
	}
}

func list(file string) error {
	kv, err := leveldb.NewStorage(file)
	if err != nil {
		return err
	}
	defer kv.Close()
	it := kv.Find("", "")
	defer it.Close()
	for it.Next() {
		fmt.Println(it.Key(), it.Value())
	}
	return nil
}

func delete(file, key string) (err error) {
	kv, err := leveldb.NewStorage(file)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := kv.Close()
		if err == nil {
			err = closeErr
		}
	}()
	return kv.Delete(key)
}
