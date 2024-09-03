// Note: run `go doc -all` in this package to see all of the types and functions available.
// ./pdk.gen.go contains the domain types from the host where your plugin will run.
package main

import "strings"


func apply() error {
    return nil
}
	
// This function copies a header into another header.
func copyHeader() error {
			// TODO: fill out your implementation here
	headerValue, err := GetHeader("the-header-in")
	if err != nil {
		return err
	}

	err = SetHeader(Header{
		Key:   "the-header-out",
		Value: headerValue,
	})
	if err != nil {
		return err
	}

	return nil
}

	
// This function copies a header to the key.
func headerToKey() error {
	headerValue, err := GetHeader("the-key")
    if err != nil {
		return err
	}

	return SetKey(string(headerValue))
}

	
// This function converts key and value to upper case.
func toUpper() error {
	k, err := GetKey()
	if err != nil {
		return err
	}

	v, err := GetValue()
	if err != nil {
		return err
	}

	SetKey(strings.ToUpper(*k))
	SetValue([]byte(strings.ToUpper(string(v))))
	return nil
}

	
// This function takes a Record and returns a Record.
func transform() error {
	input, err := GetRecord()
	if err != nil {
		return err
	}
	input.Key = input.Value
	input.Value = []byte(strings.ToUpper(string(input.Value)))

	err = SetRecord(*input)
	if err != nil {
		return err
	}

	return nil	}

	
// This function copies a value to the key.
func valueToKey() error {
	value, err := GetValue()
	if err != nil {
		return err
	}

	err = SetKey(string(value))
	if err != nil {
		return err
	}

	return nil
	}

