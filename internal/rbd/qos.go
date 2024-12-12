/*
Copyright 2024 The Ceph-CSI Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rbd

import (
	"context"
	"errors"
	"strconv"

	"github.com/ceph/ceph-csi/internal/util/log"

	librbd "github.com/ceph/go-ceph/rbd"
)

const (
	// Qos parameters name of StorageClass.
	baseReadIops            = "BaseReadIops"
	baseWriteIops           = "BaseWriteIops"
	baseReadBytesPerSecond  = "BaseReadBytesPerSecond"
	baseWriteBytesPerSecond = "BaseWriteBytesPerSecond"
	readIopsPerGB           = "ReadIopsPerGB"
	writeIopsPerGB          = "WriteIopsPerGB"
	readBpsPerGB            = "ReadBpsPerGB"
	writeBpsPerGB           = "WriteBpsPerGB"
	baseVolSizeBytes        = "BaseVolSizeBytes"

	// Qos type name of rbd image.
	readIopsLimit      = "rbd_qos_read_iops_limit"
	writeIopsLimit     = "rbd_qos_write_iops_limit"
	readBpsLimit       = "rbd_qos_read_bps_limit"
	writeBpsLimit      = "rbd_qos_write_bps_limit"
	metadataConfPrefix = "conf_"

	// The params use to calc qos based on capacity.
	baseQosReadIopsLimit  = "rbd_base_qos_read_iops_limit"
	baseQosWriteIopsLimit = "rbd_base_qos_write_iops_limit"
	baseQosReadBpsLimit   = "rbd_base_qos_read_bps_limit"
	baseQosWriteBpsLimit  = "rbd_base_qos_write_bps_limit"
	readIopsPerGBLimit    = "rbd_read_iops_per_gb_limit"
	writeIopsPerGBLimit   = "rbd_write_iops_per_gb_limit"
	readBpsPerGBLimit     = "rbd_read_bps_per_gb_limit"
	writeBpsPerGBLimit    = "rbd_write_bps_per_gb_limit"
	baseQosVolSize        = "rbd_base_qos_vol_size"
)

type qosSpec struct {
	baseLimitType  string
	baseLimit      string
	perGBLimitType string
	perGBLimit     string
	provide        bool
}

func parseQosParams(
	scParams map[string]string,
) map[string]*qosSpec {
	rbdQosParameters := map[string]*qosSpec{
		baseReadIops:            {readIopsLimit, "", readIopsPerGB, "", false},
		baseWriteIops:           {writeIopsLimit, "", writeIopsPerGB, "", false},
		baseReadBytesPerSecond:  {readBpsLimit, "", readBpsPerGB, "", false},
		baseWriteBytesPerSecond: {writeBpsLimit, "", writeBpsPerGB, "", false},
	}
	for k, v := range scParams {
		if qos, ok := rbdQosParameters[k]; ok && v != "" {
			qos.baseLimit = v
			qos.provide = true
			for _k, _v := range scParams {
				if _k == qos.perGBLimitType && _v != "" {
					qos.perGBLimit = _v
				}
			}
		}
	}

	return rbdQosParameters
}

func (rv *rbdVolume) SetQOS(
	ctx context.Context,
	scParams map[string]string,
) error {
	rv.BaseVolSize = ""
	if v, ok := scParams[baseVolSizeBytes]; ok && v != "" {
		rv.BaseVolSize = v
	}

	rbdQosParameters := parseQosParams(scParams)
	for _, qos := range rbdQosParameters {
		if qos.provide {
			err := calcQosBasedOnCapacity(ctx, rv, *qos)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (rv *rbdVolume) ApplyQOS(
	ctx context.Context,
) error {
	for k, v := range rv.QosParameters {
		err := rv.SetMetadata(metadataConfPrefix+k, v)
		if err != nil {
			log.ErrorLog(ctx, "failed to set rbd qos, %s: %s. %v", k, v, err)

			return err
		}
	}

	return nil
}

func calcQosBasedOnCapacity(
	ctx context.Context,
	rbdVol *rbdVolume,
	qos qosSpec,
) error {
	if rbdVol.QosParameters == nil {
		rbdVol.QosParameters = make(map[string]string)
	}

	// Don't set qos if base qos limit empty.
	if qos.baseLimit == "" {
		return nil
	}
	baseLimit, err := strconv.ParseInt(qos.baseLimit, 10, 64)
	if err != nil {
		log.ErrorLog(ctx, "failed to parse %s: %s. %v", qos.baseLimitType, qos.baseLimit, err)

		return err
	}

	// if provide qosPerGB and baseVolSize, we will set qos based on capacity,
	// otherwise, we only set base qos limit.
	if qos.perGBLimit != "" && rbdVol.BaseVolSize != "" {
		perGBLimit, err := strconv.ParseInt(qos.perGBLimit, 10, 64)
		if err != nil {
			log.ErrorLog(ctx, "failed to parse %s: %s. %v", qos.perGBLimitType, qos.perGBLimit, err)

			return err
		}

		baseVolSize, err := strconv.ParseInt(rbdVol.BaseVolSize, 10, 64)
		if err != nil {
			log.ErrorLog(ctx, "failed to parse %s: %s. %v", baseVolSizeBytes, rbdVol.BaseVolSize, err)

			return err
		}

		if rbdVol.VolSize <= baseVolSize {
			rbdVol.QosParameters[qos.baseLimitType] = qos.baseLimit
		} else {
			capacityQos := (rbdVol.VolSize - baseVolSize) / int64(oneGB) * perGBLimit
			finalQosLimit := baseLimit + capacityQos
			rbdVol.QosParameters[qos.baseLimitType] = strconv.FormatInt(finalQosLimit, 10)
		}
	} else {
		rbdVol.QosParameters[qos.baseLimitType] = qos.baseLimit
	}

	return nil
}

func (rv *rbdVolume) SaveQOS(
	ctx context.Context,
	scParams map[string]string,
) error {
	needSaveQosParameters := map[string]string{
		baseReadIops:            baseQosReadIopsLimit,
		baseWriteIops:           baseQosWriteIopsLimit,
		baseReadBytesPerSecond:  baseQosReadBpsLimit,
		baseWriteBytesPerSecond: baseQosWriteBpsLimit,
		readIopsPerGB:           readIopsPerGBLimit,
		writeIopsPerGB:          writeIopsPerGBLimit,
		readBpsPerGB:            readBpsPerGBLimit,
		writeBpsPerGB:           writeBpsPerGBLimit,
		baseVolSizeBytes:        baseQosVolSize,
	}
	for k, v := range scParams {
		if param, ok := needSaveQosParameters[k]; ok {
			if v != "" {
				err := rv.SetMetadata(param, v)
				if err != nil {
					log.ErrorLog(ctx, "failed to save qos. %s: %s, %v", k, v, err)

					return err
				}
			}
		}
	}

	return nil
}

func getRbdImageQOS(
	ctx context.Context,
	rbdVol *rbdVolume,
) (map[string]qosSpec, error) {
	QosParams := map[string]struct {
		rbdQosType      string
		rbdQosPerGBType string
	}{
		baseQosReadIopsLimit:  {readIopsLimit, readIopsPerGBLimit},
		baseQosWriteIopsLimit: {writeIopsLimit, writeIopsPerGBLimit},
		baseQosReadBpsLimit:   {readBpsLimit, readBpsPerGBLimit},
		baseQosWriteBpsLimit:  {writeBpsLimit, writeBpsPerGBLimit},
	}
	rbdQosParameters := make(map[string]qosSpec)
	for k, param := range QosParams {
		baseLimit, err := rbdVol.GetMetadata(k)
		if errors.Is(err, librbd.ErrNotFound) {
			// if base qos dose not exist, skipping.
			continue
		} else if err != nil {
			log.ErrorLog(ctx, "failed to get metadata: %s. %v", k, err)

			return nil, err
		}
		perGBLimit, err := rbdVol.GetMetadata(param.rbdQosPerGBType)
		if errors.Is(err, librbd.ErrNotFound) {
			// rbdQosPerGBType does not exist, set it empty.
			perGBLimit = ""
		} else if err != nil {
			log.ErrorLog(ctx, "failed to get metadata: %s. %v", param.rbdQosPerGBType, err)

			return nil, err
		}
		rbdQosParameters[k] = qosSpec{param.rbdQosType, baseLimit, param.rbdQosPerGBType, perGBLimit, true}
	}
	baseVolSize, err := rbdVol.GetMetadata(baseQosVolSize)
	if errors.Is(err, librbd.ErrNotFound) {
		// rbdBaseQosVolSize does not exist, set it empty.
		baseVolSize = ""
	} else if err != nil {
		log.ErrorLog(ctx, "failed to get metadata: %s. %v", baseQosVolSize, err)

		return nil, err
	}
	rbdVol.BaseVolSize = baseVolSize

	return rbdQosParameters, nil
}

func (rv *rbdVolume) AdjustQOS(
	ctx context.Context,
) error {
	rbdQosParameters, err := getRbdImageQOS(ctx, rv)
	if err != nil {
		log.ErrorLog(ctx, "get rbd image qos failed")

		return err
	}
	for _, param := range rbdQosParameters {
		err = calcQosBasedOnCapacity(ctx, rv, param)
		if err != nil {
			return err
		}
	}
	err = rv.ApplyQOS(ctx)
	if err != nil {
		return err
	}

	return nil
}
