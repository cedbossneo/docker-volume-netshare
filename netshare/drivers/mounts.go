package drivers

import (
	"encoding/json"
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/volume"
	"github.com/hashicorp/consul/api"
	"strings"
	"os"
)

const (
	ShareOpt  = "share"
	CreateOpt = "create"
)

type mount struct {
	Name        string
	HostDir     string
	Opts        map[string]string
	Managed     bool
	Connections map[string]int
}

type mountManager struct {
	consulClient  *api.Client
	consulKV      *api.KV
	consulBaseKey string
	host		  string
}

func NewVolumeManager(consulAddress string, consulToken string, consulBaseKey string) *mountManager {
	consulClient, err := api.NewClient(&api.Config{
		Address: consulAddress,
		Token:   consulToken,
	})
	if err != nil {
		panic(err)
	}
	consulKV := consulClient.KV()
	log.Info("Consul ready on ", consulAddress, " with baseKey ", consulBaseKey)
	host, _ := os.Hostname();
	return &mountManager{consulClient: consulClient, consulKV: consulKV, consulBaseKey: consulBaseKey, host: host}
}

func (m *mountManager) getConsulMount(name string) *mount {
	key, _, err := m.consulKV.Get(m.consulBaseKey+name, nil)
	if err != nil {
		log.Error(err)
	}
	if key == nil {
		return nil
	}
	mount := mount{}
	json.Unmarshal(key.Value, &mount)
	log.Info("Retrieve mount ", mount.Name, " from consul")
	return &mount
}

func (m *mountManager) putConsulMount(mount *mount) error {
	key, _, err := m.consulKV.Get(m.consulBaseKey+mount.Name, nil)
	if err != nil {
		log.Error(err)
		return err
	}
	if key == nil {
		key = &api.KVPair{Key: m.consulBaseKey + mount.Name}
	}
	jsonMount, _ := json.Marshal(mount)
	key.Value = jsonMount
	_, err = m.consulKV.Put(key, nil)
	log.Info("Put mount ", mount.Name, " in consul")
	return err
}

func (m *mountManager) deleteConsulMount(name string) error {
	_, err := m.consulKV.Delete(m.consulBaseKey+name, nil)
	if err != nil {
		log.Error(err)
	}
	log.Info("Delete mount ", name, " from consul")
	return err
}

func (m *mountManager) HasMount(name string) bool {
	mount := m.getConsulMount(name)
	return mount != nil
}

func (m *mountManager) HasOptions(name string) bool {
	mount := m.getConsulMount(name)
	if mount != nil {
		return mount.Opts != nil && len(mount.Opts) > 0
	}
	return false
}

func (m *mountManager) HasOption(name, key string) bool {
	if m.HasOptions(name) {
		mount := m.getConsulMount(name)
		if _, ok := mount.Opts[key]; ok {
			return ok
		}
	}
	return false
}

func (m *mountManager) GetOptions(name string) map[string]string {
	if m.HasOptions(name) {
		mount := m.getConsulMount(name)
		return mount.Opts
	}
	return map[string]string{}
}

func (m *mountManager) GetOption(name, key string) string {
	if m.HasOption(name, key) {
		mount := m.getConsulMount(name)
		v, _ := mount.Opts[key]
		return v
	}
	return ""
}

func (m *mountManager) GetOptionAsBool(name, key string) bool {
	rv := strings.ToLower(m.GetOption(name, key))
	if rv == "yes" || rv == "true" {
		return true
	}
	return false
}

func (m *mountManager) IsActiveMount(name string) bool {
	mount := m.getConsulMount(name)
	return mount != nil && mount.Connections[m.host] > 0
}

func (m *mountManager) Count(name string) int {
	mount := m.getConsulMount(name)
	if mount != nil {
		return mount.Connections[m.host]
	}
	return 0
}

func (m *mountManager) Add(name, hostdir string) {
	mnt := m.getConsulMount(name)
	if mnt != nil {
		m.Increment(name)
	} else {
		c := map[string]int{}
		c[m.host] = 1
		mnt := &mount{Name: name, HostDir: hostdir, Managed: false, Connections: c}
		m.putConsulMount(mnt)
	}
}

func (m *mountManager) Create(name, hostdir string, opts map[string]string) *mount {
	mnt := m.getConsulMount(name)
	if mnt != nil && mnt.Connections[m.host] > 0 {
		mnt.Opts = opts
		m.putConsulMount(mnt)
		return mnt
	} else {
		c := map[string]int{}
		c[m.host] = 0
		mnt := &mount{Name: name, HostDir: hostdir, Managed: true, Opts: opts, Connections: c}
		m.putConsulMount(mnt)
		return mnt
	}
}

func (m *mountManager) Delete(name string) error {
	log.Debugf("Delete volume: %s, connections: %d", name, m.Count(name))
	if m.HasMount(name) {
		if m.Count(name) < 1 {
			m.deleteConsulMount(name)
			return nil
		}
		return errors.New("Volume is currently in use")
	}
	m.deleteConsulMount(name)
	return nil
}

func (m *mountManager) DeleteIfNotManaged(name string) error {
	if m.HasMount(name) && !m.IsActiveMount(name) {
		mount := m.getConsulMount(name)
		if mount.Managed {
			return nil
		}
		log.Infof("Removing un-Managed volume")
		return m.Delete(name)
	}
	return nil
}

func (m *mountManager) Increment(name string) int {
	mount := m.getConsulMount(name)
	if mount != nil {
		mount.Connections[m.host]++
		m.putConsulMount(mount)
		return mount.Connections[m.host]
	}
	return 0
}

func (m *mountManager) Decrement(name string) int {
	mount := m.getConsulMount(name)
	if mount != nil && mount.Connections[m.host] > 0 {
		mount.Connections[m.host]--
		m.putConsulMount(mount)
	}
	return 0
}

func (m *mountManager) GetVolumes(rootPath string) []*volume.Volume {

	volumes := []*volume.Volume{}

	keys, _, _ := m.consulKV.List(m.consulBaseKey, nil)
	log.Info("List mounts from consul")
	if keys == nil {
		return volumes
	}
	for _, val := range keys {
		mount := mount{}
		json.Unmarshal(val.Value, &mount)
		volumes = append(volumes, &volume.Volume{Name: mount.Name, Mountpoint: mount.HostDir})
	}
	return volumes
}

func (n cephDriver) isMounted(path string) error {
	cmd := "grep -qs '"+path+"' /proc/mounts"
	return run(cmd)
}
