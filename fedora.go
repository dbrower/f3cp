// Package fedora provides a thin wrapper around the Fedora REST API.
package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Exported errors
var (
	ErrNotFound      = errors.New("Item Not Found in Fedora")
	ErrNotAuthorized = errors.New("Access Denied")
	ErrNeedPID       = errors.New("PID missing")
)

// Fedora represents a Fedora Commons server. The exact nature of the
// server is unspecified.
type Fedora interface {
	// Return the contents of the dsname datastream of object id.
	// You are expected to close it when you are finished.
	//GetDatastream(id, dsname string) (io.ReadCloser, ContentInfo, error)
	// GetDatastreamInfo returns the metadata Fedora stores about the named
	// datastream.
	GetDatastreamInfo(id, dsname string) (DsInfo, error)
}

// NewRemote creates a reference to a remote Fedora repository.
// fedoraPath is a complete URL including username and password, if necessary.
// For example
//	http://fedoraAdmin:password@localhost:8983/fedora/
// The returned structure does not buffer or cache Fedora responses.
func NewRemote(fedoraPath string) *remoteFedora {
	rf := &remoteFedora{hostpath: fedoraPath}
	if rf.hostpath[len(rf.hostpath)-1] != '/' {
		rf.hostpath = rf.hostpath + "/"
	}
	return rf
}

type remoteFedora struct {
	hostpath string
}

type ObjectInfo struct {
	PID         string    `xml:"pid,attr"`
	Label       string    `xml:"objLabel"`
	CreateDate  time.Time `xml:"objCreateDate"`
	LastModDate time.Time `xml:"ObjLastModDate"`
	State       string    `xml:"objState"`
}

type DSList struct {
	DS []struct {
		Name string `xml:"dsid,attr"`
	} `xml:"datastream"`
}

func (rf *remoteFedora) GetObjectInfo(id string) (ObjectInfo, error) {
	// TODO: make this joining smarter wrt not duplicating slashes
	var path = rf.hostpath + "objects/" + id + "?format=xml"
	var info ObjectInfo
	err := rf.getXML(path, &info)
	return info, err
}

func (rf *remoteFedora) GetDatastreamList(id string) ([]string, error) {
	var result []string
	var dslist DSList
	path := rf.hostpath + "objects/" + id + "/datastreams?format=xml"
	err := rf.getXML(path, &dslist)
	for i := range dslist.DS {
		result = append(result, dslist.DS[i].Name)
	}
	return result, err
}

// DsInfo holds more complete metadata on a datastream (as opposed to the
// ContentInfo structure)
type DsInfo struct {
	Name         string `xml:"dsID,attr"`
	Label        string `xml:"dsLabel"`
	VersionID    string `xml:"dsVersionID"`
	State        string `xml:"dsState"`
	Checksum     string `xml:"dsChecksum"`
	ChecksumType string `xml:"dsChecksumType"`
	MIMEType     string `xml:"dsMIME"`
	Location     string `xml:"dsLocation"`
	LocationType string `xml:"dsLocationType"`
	ControlGroup string `xml:"dsControlGroup"`
	Versionable  bool   `xml:"dsVersionable"`
	Size         int    `xml:"dsSize"`
}

func (rf *remoteFedora) GetDatastreamInfo(id, dsname string) (DsInfo, error) {
	// TODO: make this joining smarter wrt not duplicating slashes
	var path = rf.hostpath + "objects/" + id + "/datastreams/" + dsname + "?format=xml"
	var info DsInfo
	err := rf.getXML(path, &info)
	// Why must fedora return "none" when there is no checksum??
	if info.Checksum == "none" {
		info.Checksum = ""
	}
	return info, err
}

func (rf *remoteFedora) getXML(path string, result interface{}) error {
	r, err := http.Get(path)
	if err != nil {
		return err
	}
	if r.StatusCode != 200 {
		r.Body.Close()
		switch r.StatusCode {
		case 404:
			return ErrNotFound
		case 401:
			return ErrNotAuthorized
		default:
			return fmt.Errorf("Received status %d from fedora", r.StatusCode)
		}
	}
	dec := xml.NewDecoder(r.Body)
	err = dec.Decode(&result)
	r.Body.Close()
	return err
}

// returns the contents of the datastream `dsname`.
// The returned stream needs to be closed when finished.
func (rf *remoteFedora) GetDatastream(id, dsname string) (io.ReadCloser, error) {
	// TODO: make this joining smarter wrt not duplicating slashes
	var path = rf.hostpath + "objects/" + id + "/datastreams/" + dsname + "/content"
	r, err := http.Get(path)
	if err != nil {
		return nil, err
	}
	if r.StatusCode != 200 {
		r.Body.Close()
		switch r.StatusCode {
		case 404:
			return nil, ErrNotFound
		case 401:
			return nil, ErrNotAuthorized
		default:
			return nil, fmt.Errorf("Received status %d from fedora", r.StatusCode)
		}
	}
	return r.Body, nil
}

func (rf *remoteFedora) MakeObject(info ObjectInfo) error {
	if info.PID == "" {
		return ErrNeedPID
	}
	params := url.Values{}
	if info.Label != "" {
		params.Set("label", info.Label)
	}
	path := rf.hostpath + "objects/" + info.PID + "?" + params.Encode()
	response, err := http.Post(path, "", nil)
	if err != nil {
		return err
	}
	switch response.StatusCode {
	case 201:
		return nil
	case 404:
		return ErrNotFound
	case 401:
		return ErrNotAuthorized
	default:
		return fmt.Errorf("Received status %d from fedora", response.StatusCode)
	}
}

func (rf *remoteFedora) MakeDatastream(id string, info DsInfo, content io.Reader) error {
	return rf.updateDS(id, info, content, "POST")
}

func (rf *remoteFedora) updateDS(id string, info DsInfo, content io.Reader, verb string) error {
	params := url.Values{}
	if info.Label != "" {
		params.Set("dsLabel", info.Label)
	}
	if info.ControlGroup != "" {
		params.Set("controlGroup", info.ControlGroup)
	}
	//if info.Checksum != "" {
	//	params.Set("checksum", info.Checksum)
	//}
	//if info.ChecksumType != "" {
	//	params.Set("checksumType", info.ChecksumType)
	//}
	if info.LocationType == "URL" {
		params.Set("dsLocation", info.Location)
	}
	if info.MIMEType != "" {
		params.Set("mimeType", info.MIMEType)
	}
	path := rf.hostpath + "objects/" + id + "/datastreams/" + info.Name + "?" + params.Encode()
	request, err := http.NewRequest(verb, path, content)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	switch response.StatusCode {
	case 200, 201:
		return nil
	case 404:
		return ErrNotFound
	case 401:
		return ErrNotAuthorized
	default:
		return fmt.Errorf("Received status %d from fedora", response.StatusCode)
	}
}

func (rf *remoteFedora) UpdateDatastream(id string, info DsInfo, content io.Reader) error {
	return rf.updateDS(id, info, content, "PUT")
}

// NewTestFedora creates an empty TestFedora object.
func NewTestFedora() *TestFedora {
	return &TestFedora{data: make(map[string]dsPair)}
}

// TestFedora implements a simple in-memory Fedora stub which will return bytes which have
// already been specified by Set().
// Intended for testing. (Maybe move to a testing file?)
type TestFedora struct {
	data map[string]dsPair
}

type dsPair struct {
	info    DsInfo
	content []byte
}

// GetDatastream returns a ReadCloser which holds the content of the named
// datastream on the given fedora object.
//func (tf *TestFedora) GetDatastream(id, dsname string) (io.ReadCloser, ContentInfo, error) {
//	ci := ContentInfo{}
//	key := id + "/" + dsname
//	v, ok := tf.data[key]
//	if !ok {
//		return nil, ci, ErrNotFound
//	}
//	ci.Type = "text/plain"
//	ci.Length = v.info.Size
//	return ioutil.NopCloser(bytes.NewReader(v.content)), ci, nil
//}

// GetDatastreamInfo returns Fedora's metadata for the given datastream.
func (tf *TestFedora) GetDatastreamInfo(id, dsname string) (DsInfo, error) {
	key := id + "/" + dsname
	v, ok := tf.data[key]
	if !ok {
		return DsInfo{}, ErrNotFound
	}
	return v.info, nil
}

// Set the given datastream to have the given content.
func (tf *TestFedora) Set(id, dsname string, info DsInfo, value []byte) {
	if info.State == "" {
		info.State = "A"
	}
	if info.VersionID == "" {
		info.VersionID = dsname + ".0"
	}
	if info.Location == "" {
		info.Location = fmt.Sprintf("%s+%s+%s", id, dsname, info.VersionID)
	}
	if info.LocationType == "" {
		info.LocationType = "INTERNAL_ID"
	}
	//if info.Size == "" {
	//	info.Size = fmt.Sprintf("%d", len(value))
	//}
	key := id + "/" + dsname
	tf.data[key] = dsPair{info, value}
}
