package bondremote

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"strings"

	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioRemoteStorage struct {
	client     *minio.Client
	bucketName string
}

func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
func NewMinioRemote() *MinioRemoteStorage {
	client, err := minio.New("172.17.0.2:9000", &minio.Options{Creds: credentials.NewStaticV4("minioadmin", "minioadmin", "")})
	if err != nil {
		panic(err)
	}
	name := RandomString(5)
	err = client.MakeBucket(context.TODO(), name, minio.MakeBucketOptions{})
	if err != nil {
		panic(err)
	}
	return &MinioRemoteStorage{
		client:     client,
		bucketName: name,
	}
}

var _ remote.Storage = &MinioRemoteStorage{}

func (m *MinioRemoteStorage) Close() error {
	return nil
}

func (m *MinioRemoteStorage) ReadObject(ctx context.Context, objName string) (remote.ObjectReader, int64, error) {
	obj, err := m.client.GetObject(ctx, m.bucketName, objName, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, err
	}
	stat, err := obj.Stat()
	if err != nil {
		return nil, 0, err
	}
	return &MinioObjectReader{obj: obj}, stat.Size, nil
}

func (m *MinioRemoteStorage) CreateObject(objName string) (io.WriteCloser, error) {
	return &MinioObjectWriter{
		storate:    m,
		objectName: objName,
		buf:        &bytes.Buffer{},
	}, nil
}

func (m *MinioRemoteStorage) List(prefix, delimiter string) ([]string, error) {
	if delimiter != "" {
		panic("delimiter unimplemented")
	}

	infos := m.client.ListObjects(context.Background(), m.bucketName, minio.ListObjectsOptions{
		Prefix: prefix,
	})
	objs := []string{}
	for info := range infos {
		objs = append(objs, info.Key)
	}
	return objs, nil
}

func (m *MinioRemoteStorage) Delete(objName string) error {
	err := m.client.RemoveObject(context.TODO(), m.bucketName, objName, minio.RemoveObjectOptions{})
	return err
}

func (m *MinioRemoteStorage) Size(objName string) (int64, error) {
	_, size, err := m.ReadObject(context.TODO(), objName)
	return size, err
}

func (m *MinioRemoteStorage) IsNotExistError(err error) bool {
	return strings.Contains(err.Error(), "does not exist")
}

type MinioObjectReader struct {
	obj *minio.Object
}

func (o *MinioObjectReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	n, err := o.obj.ReadAt(p, offset)
	if n == len(p) && err == io.EOF {
		return nil
	}
	return err
}

func (o *MinioObjectReader) Close() error {
	return o.obj.Close()
}

type MinioObjectWriter struct {
	buf        *bytes.Buffer
	objectName string
	storate    *MinioRemoteStorage
}

func (w *MinioObjectWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *MinioObjectWriter) Close() error {
	_, err := w.storate.client.PutObject(context.TODO(), w.storate.bucketName, w.objectName, w.buf, int64(w.buf.Len()), minio.PutObjectOptions{})
	return err
}
