// Copyright 2025 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

type CommandLineConfig struct {
	Host             string
	Port             int
	UnixSocket       string
	Username         string
	Password         string
	Database         string
	RetentionPolicy  string
	Timeout          int
	EnableTls        bool
	InsecureTls      bool
	CACert           string
	Cert             string
	CertKey          string
	InsecureHostname bool

	precision string
}
