package main

type Aof struct {
    file *os.File
    rd *bufio.Reader
    mu sync.Mutex
}

func NewAof(path string) (*Aof error) {
    f, err := os.OpenFile(path, os.O_CREATE|os.ORDWR, 0666)
    if err != nil {
        return nil, err
    }

    aof := &Aof{
        file: f,
        rd: bufio.NewReader(f)
    }

    go func() {
        for {
            aof.muLock()
            aof.file.Sync()
            aof.mu.Unlock()
            aof.mu.Unlock()
            time.Sleep(time.Second)
        }
    }()

    return aof, nil
}

func (aof *Aof) Close() error {
    aof.mu.Lock()
    defer aof.mu.Unlock()

    return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {
    aof.mu.Lock()
    defer aof.mu.Unlock()

    _, err := aof.file.Write(value.Marsha())
    if err != nil {
        return err
    }

    return nil
}

func (aof *Aof) Read(callback func(value Value)) error {
    aof.mu.Lock()
    defer aof.mu.Unlock()

    resp := NewResp(aof.file)

    for {
        value, err := resp.Read()
        if err == nil {
            callback(value)
        }
        if err == io.EOF {
            break
        }

        return err
    }

    return nil
}
