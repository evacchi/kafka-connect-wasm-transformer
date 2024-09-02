// Note: run `go doc -all` in this package to see all of the types and functions available.
// ./pdk.gen.go contains the domain types from the host where your plugin will run.
package main

import "strings"

// This function transforms a given record in place
func apply() error {
	return nil
}

// This function convert key and value to upper case.
func to_upper() error {
	k, err := Get_key()
	if err != nil {
		return err
	}

	v, err := Get_value()
	if err != nil {
		return err
	}

	Set_key(strings.ToUpper(*k))
	Set_value([]byte(strings.ToUpper(string(v))))
	return nil
}

// This function convert key and value to upper case.
func copy_header() error {
	headerValue, err := Get_header("the-header-in")
	if err != nil {
		return err
	}

	err = Set_header(Header{
		Key:   "the-header-out",
		Value: headerValue,
	})
	if err != nil {
		return err
	}

	return nil
}

func value_to_key() error {
	value, err := Get_value()
	if err != nil {
		return err
	}

	err = Set_key(string(value))
	if err != nil {
		return err
	}

	return nil
}

func header_to_key() error {
	headerValue, err := Get_header("the-key")
	if err != nil {
		return err
	}

	err = Set_key(string(headerValue))
	if err != nil {
		return err
	}

	return nil
}

// This function takes a Record and returns a Record.
// It takes Record as input (A plain key/value record.)
// And returns Record (A plain key/value record.)
func transform() error {
	input, err := Get_record()
	if err != nil {
		return err
	}
	input.Key = input.Value
	input.Value = []byte(strings.ToUpper(string(input.Value)))

	err = Set_record(*input)
	if err != nil {
		return err
	}

	return nil
}
