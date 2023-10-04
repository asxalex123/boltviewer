all:
	go build -ldflags '-s -w' -o boltviewer
	upx boltviewer
	sudo cp boltviewer /usr/bin
