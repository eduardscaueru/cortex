package ruler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

// TODO: Extract configs client logic into go client library (ala users)

type configID int

type cortexConfig struct {
	RulesFiles map[string]string `json:"rules_files"`
}

type cortexConfigView struct {
	ConfigID configID     `json:"id"`
	Config   cortexConfig `json:"config"`
}

// cortexConfigsResponse is a response from server for getOrgConfigs
type cortexConfigsResponse struct {
	// Configs maps organization ID to their latest cortexConfigView.
	Configs map[string]cortexConfigView `json:"configs"`
}

func configsFromJSON(body io.Reader) (map[string]cortexConfigView, error) {
	var configs cortexConfigsResponse
	if err := json.NewDecoder(body).Decode(&configs); err != nil {
		log.Errorf("configs: couldn't decode JSON body: %v", err)
		return nil, err
	}
	log.Debugf("configs: got response: %v", configs)
	return configs.Configs, nil
}

// getLatestConfigID returns the last config ID from a set of configs.
func getLatestConfigID(configs map[string]cortexConfigView) configID {
	latest := configID(0)
	for _, config := range configs {
		if config.ConfigID > latest {
			latest = config.ConfigID
		}
	}
	return latest
}

// Get the rules from the cortex configuration.
//
// Strongly inspired by `loadGroups` in Prometheus.
func (c cortexConfig) GetRules() ([]rules.Rule, error) {
	result := []rules.Rule{}
	for fn, content := range c.RulesFiles {
		stmts, err := promql.ParseStmts(content)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %s", fn, err)
		}

		for _, stmt := range stmts {
			var rule rules.Rule

			switch r := stmt.(type) {
			case *promql.AlertStmt:
				rule = rules.NewAlertingRule(r.Name, r.Expr, r.Duration, r.Labels, r.Annotations)

			case *promql.RecordStmt:
				rule = rules.NewRecordingRule(r.Name, r.Expr, r.Labels)

			default:
				return nil, fmt.Errorf("ruler.GetRules: unknown statement type")
			}
			result = append(result, rule)
		}
	}
	return result, nil
}

type configsAPI struct {
	url *url.URL
}

// getOrgConfigs returns all Cortex configurations from a configs api server
// that have been updated since the given time.
func (c *configsAPI) getOrgConfigs(since configID) (map[string]cortexConfigView, error) {
	suffix := ""
	if since != 0 {
		suffix = fmt.Sprintf("?since=%d", since)
	}
	url := fmt.Sprintf("%s/private/api/configs/org/cortex%s", c.url.String(), suffix)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	res, err := client.Do(req)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Invalid response from configs server: %v", res.StatusCode)
	}
	return configsFromJSON(res.Body)
}
