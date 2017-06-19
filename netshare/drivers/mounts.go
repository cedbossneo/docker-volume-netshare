package drivers

import (
	"encoding/json"
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/volume"
	consulApi "github.com/hashicorp/consul/api"
	vaultApi "github.com/hashicorp/vault/api"
	"strings"
	"os"
	"net/http"
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
	consulConfig *ConsulConfig
	consulClient  *consulApi.Client
	consulKV	 *consulApi.KV
	vaultConfig *VaultConfig
	vaultClient *vaultApi.Client
	host		  string
}

func NewVolumeManager(consulConfig *ConsulConfig, vaultConfig *VaultConfig) *mountManager {
	consulClient, consulKV := createConsulClient(consulConfig)
	vaultClient := createVaultClient(vaultConfig)
	host, _ := os.Hostname();
	return &mountManager{vaultConfig: vaultConfig, vaultClient: vaultClient, consulClient: consulClient, consulConfig: consulConfig, consulKV: consulKV, host: host}
}

func createVaultClient(vaultConfig *VaultConfig) *vaultApi.Client {
	config := vaultApi.DefaultConfig()
	config.Address = vaultConfig.Address
	config.HttpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify = true
	vaultClient, err := vaultApi.NewClient(config)
	if err != nil {
		log.Fatal("err: %s", err)
		return nil
	}
	log.Info("Created Vault Client. Address: ", vaultConfig.Address)
	return vaultClient
}

func createConsulClient(consulConfig *ConsulConfig) (*consulApi.Client, *consulApi.KV) {
	config := consulApi.DefaultConfig()
	config.Address = consulConfig.Address
	config.Token = consulConfig.Token
	consulClient, err := consulApi.NewClient(config)
	if err != nil {
		panic(err)
	}
	consulKV := consulClient.KV()
	log.Info("Consul ready on ", consulConfig.Address, " with baseKey ", consulConfig.BaseKey)
	return consulClient, consulKV
}

func (m *mountManager) getConsulMount(name string) *mount {
	key, _, err := m.consulKV.Get(m.consulConfig.BaseKey+name, nil)
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

func (m *mountManager) getVaultConfig(name string) map[string]interface{} {
	if m.vaultClient == nil {
		return nil
	}
	secret, err := m.vaultClient.Logical().Write("auth/approle/login", map[string]interface{}{
		"role_id": m.vaultConfig.RoleId,
		"secret_id": m.vaultConfig.SecretId,
	})
	if err != nil {
		log.Println(err)
		return nil
	}
	m.vaultClient.SetToken(secret.Auth.ClientToken)
	secret, err = m.vaultClient.Logical().Read(m.vaultConfig.BaseKey + name)
	return secret.Data
}

func (m *mountManager) FillVaultConfigInMount(name string) *mount {
	mount := m.getConsulMount(name)
	data := m.getVaultConfig(name)
	if data != nil {
		for key, val := range data {
			mount.Opts[key] = val.(string)
		}
	}
	return mount
}

func (m *mountManager) FillVaultConfigInOpts(name string, opts map[string]string) map[string]string {
	data := m.getVaultConfig(name)
	if data == nil {
		return opts
	}
	for key, val := range data {
		opts[key] = val.(string)
	}
	return opts
}

func (m *mountManager) putConsulMount(mount *mount) error {
	key, _, err := m.consulKV.Get(m.consulConfig.BaseKey+mount.Name, nil)
	if err != nil {
		log.Error(err)
		return err
	}
	if key == nil {
		key = &consulApi.KVPair{Key: m.consulConfig.BaseKey + mount.Name}
	}
	jsonMount, _ := json.Marshal(mount)
	key.Value = jsonMount
	_, err = m.consulKV.Put(key, nil)
	log.Info("Put mount ", mount.Name, " in consul")
	return err
}

func (m *mountManager) deleteConsulMount(name string) error {
	_, err := m.consulKV.Delete(m.consulConfig.BaseKey+name, nil)
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

	keys, _, _ := m.consulKV.List(m.consulConfig.BaseKey, nil)
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
