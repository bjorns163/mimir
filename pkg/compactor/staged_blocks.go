package compactor

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type stagedBlock struct {
	meta            *block.Meta
	targetLabelSets []labels.Labels
	blockDir        string
}

func (b *stagedBlock) isApplicable(ctx context.Context, meta *block.Meta, blockDir string) (bool, error) {
	// Exit early if time range doesn't overlap.
	if b.meta.BlockMeta.MinTime > meta.BlockMeta.MaxTime || b.meta.BlockMeta.MaxTime < meta.BlockMeta.MinTime {
		return false, nil
	}

	// The time range overlaps so now we search for the target labels.
	idxReader, err := index.NewFileReader(filepath.Join(blockDir, block.IndexFilename))
	if err != nil {
		return false, errors.Wrapf(err, "open index file for block %s", blockDir)
	}
	defer func() { _ = idxReader.Close() }()

	n, v := index.AllPostingsKey()
	postings, err := idxReader.Postings(ctx, n, v)
	if err != nil {
		return false, errors.Wrapf(err, "get all postings for block %s", blockDir)
	}

	var (
		lbls       labels.Labels
		lblBuilder labels.ScratchBuilder
		chks       []chunks.Meta
	)

	for postings.Next() {
		sRef := postings.At()
		if err := idxReader.Series(sRef, &lblBuilder, &chks); err != nil {
			return false, errors.Wrap(err, "read series")
		}

		lbls = lblBuilder.Labels()

		for _, targetLabels := range b.targetLabelSets {
			if labels.Equal(targetLabels, lbls) {
				return true, nil
			}
		}
	}

	return false, nil
}

type stagedBlocks struct {
	bkt       objstore.Bucket
	logger    log.Logger
	stagedDir string
	staged    []stagedBlock
}

func newStagedBlocks(bkt objstore.Bucket, logger log.Logger, stagedDir string) *stagedBlocks {
	return &stagedBlocks{
		bkt:       bkt,
		logger:    logger,
		stagedDir: stagedDir,
	}
}

// searchForApplicable returns a map of target labels to staged blocks.
func (sb *stagedBlocks) searchForApplicable(ctx context.Context, meta *block.Meta, blockDir string) (map[string]stagedBlock, error) {
	res := make(map[string]stagedBlock)

	for _, staged := range sb.staged {
		applicable, err := staged.isApplicable(ctx, meta, blockDir)
		if err != nil {
			return nil, err
		}

		if applicable {
			for _, tls := range staged.targetLabelSets {
				res[tls.String()] = staged
			}
		}
	}

	return res, nil
}

func (sb *stagedBlocks) addStagedBlocks(ctx context.Context, metas map[ulid.ULID]*block.Meta) error {
	for blockID, meta := range metas {
		blockDir := filepath.Join(sb.stagedDir, blockID.String())

		if err := block.Download(ctx, sb.logger, sb.bkt, blockID, blockDir); err != nil {
			return errors.Wrapf(err, "download block %s", blockID.String())
		}

		targetLabelSets, err := getTargetLabels(meta)
		if err != nil {
			return errors.Wrapf(err, "get target labels for staged block %s", blockID.String())
		}

		sb.staged = append(sb.staged, stagedBlock{
			meta:            meta,
			targetLabelSets: targetLabelSets,
			blockDir:        blockDir,
		})
	}

	return nil
}

func getTargetLabels(meta *block.Meta) ([]labels.Labels, error) {
	stagedMarkFile := block.StagedMarkFilepath(meta.ULID)

	r, err := os.Open(stagedMarkFile)
	if err != nil {
		return nil, errors.Wrapf(err, "open staged mark file %s", stagedMarkFile)
	}
	stagedBuf, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read staged mark file %s", stagedMarkFile)
	}
	_ = r.Close()

	var stagedInfo block.StagedMark
	if err = json.Unmarshal(stagedBuf, &stagedInfo); err != nil {
		return nil, errors.Wrapf(err, "unmarshal staged mark file %s", stagedMarkFile)
	}

	return stagedInfo.TargetLabelSets, nil
}
