package act

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/beekeeper/pkg/bee"
	"github.com/ethersphere/beekeeper/pkg/bee/api"
	"github.com/ethersphere/beekeeper/pkg/beekeeper"
	"github.com/ethersphere/beekeeper/pkg/logging"
	"github.com/ethersphere/beekeeper/pkg/orchestration"
	"github.com/ethersphere/beekeeper/pkg/random"
)

// Options represents check options
type Options struct {
	FileName      string
	FileSize      int64
	PostageAmount int64
	PostageDepth  uint64
	PostageLabel  string
	Seed          int64
}

// NewDefaultOptions returns new default options
func NewDefaultOptions() Options {
	return Options{
		FileName:      "act",
		FileSize:      1 * 1024,
		PostageAmount: 420000000,
		PostageDepth:  20,
		PostageLabel:  "act-label",
		Seed:          0,
	}
}

// compile check whether Check implements interface
var _ beekeeper.Action = (*Check)(nil)

// Check instance
type Check struct {
	logger logging.Logger
}

// NewCheck returns new check
func NewCheck(logger logging.Logger) beekeeper.Action {
	return &Check{
		logger: logger,
	}
}

// Run executes act check
func (c *Check) Run(ctx context.Context, cluster orchestration.Cluster, opts interface{}) (err error) {
	o, ok := opts.(Options)
	if !ok {
		return fmt.Errorf("invalid options type")
	}

	clients, err := cluster.NodesClients(ctx)
	if err != nil {
		return err
	}

	fullNodes := cluster.FullNodeNames()
	lightNodes := cluster.LightNodeNames()

	upNodeName := fullNodes[0]
	upClient := clients[upNodeName]
	addr, _ := upClient.Addresses(ctx)
	publisher, _ := swarm.ParseHexAddress(addr.PublicKey)

	nodeName1 := lightNodes[0]
	client1 := clients[nodeName1]
	addr1, _ := client1.Addresses(ctx)
	pubk1, _ := swarm.ParseHexAddress(addr1.PublicKey)

	//nodeName2 := fullNodes[1]
	//client2 := clients[nodeName2]
	//addr2, _ := client2.Addresses(ctx)
	//pubk2, _ := swarm.ParseHexAddress(addr2.PublicKey)

	//nodeName3 := fullNodes[2]
	//client3 := clients[nodeName3]
	//addr3, _ := client3.Addresses(ctx)
	//pubk3, _ := swarm.ParseHexAddress(addr3.PublicKey)

	rnds := random.PseudoGenerators(o.Seed, 1)

	fileName := fmt.Sprintf("%s-%s-%d", o.FileName, upNodeName, rnds[0].Int())
	postagelabel := fmt.Sprintf("%s-%s-%d", o.PostageLabel, upNodeName, rnds[0].Int())

	file := bee.NewRandomFile(rnds[0], fileName, o.FileSize)

	batchID, err := upClient.GetOrCreateBatch(ctx, o.PostageAmount, o.PostageDepth, postagelabel)
	if err != nil {
		return fmt.Errorf("created batched id %w", err)
	}

	// upload act file
	// ----------------------------------------------
	// Given batch is used for the upload
	// When the file is uploaded to the node
	// Then the file is uploaded successfully
	uErr := upClient.UploadActFile(ctx, &file, api.UploadOptions{BatchID: batchID})
	if uErr != nil {
		return fmt.Errorf("node %s: %w", upNodeName, uErr)
	}
	c.logger.Info("ACT file uploaded")
	time.Sleep(1 * time.Second)

	// download act file
	// ----------------------------------------------
	// Given the file is uploaded to the node
	// When the file is downloaded from the node
	// Then the file is downloaded successfully
	act := true
	fileAddress := file.Address()
	history := file.HistroryAddress()
	size, hash, err := upClient.DownloadActFile(ctx, fileAddress, &api.DownloadOptions{Act: &act, ActPublicKey: &publisher, ActHistoryAddress: &history})
	if err != nil {
		return fmt.Errorf("node %s: %w", upNodeName, err)
	}
	if !bytes.Equal(file.Hash(), hash) {
		c.logger.Infof("Node %s. ACT file hash not equal. Uploaded size: %d Downloaded size: %d  File: %s", upNodeName, file.Size(), size, fileAddress.String())
		return errors.New("ACT file retrieval - hash error")
	}
	c.logger.Info("ACT file downloaded")

	// download act file with wrong public key
	// ----------------------------------------------
	// Given the file is uploaded to the node
	// When the file is downloaded from the node with wrong public key
	// Then the file download is denied
	notPublisher := pubk1
	_, _, notPErr := upClient.DownloadActFile(ctx, file.Address(), &api.DownloadOptions{Act: &act, ActPublicKey: &notPublisher, ActHistoryAddress: &history})
	if notPErr == nil {
		return fmt.Errorf("node %s: File downloaded with wrong public key successfully - this is an error", upNodeName)
	}
	c.logger.Info("ACT Access denied with incorrect public key")
	/*

		// add grantees list
		// ----------------------------------------------
		// Given the file is uploaded to the node (file.HistoryAddress())
		// When the grantees are added to the file
		// Then the grantees are added successfully
		gFile := bee.NewBufferFile("grantees.json", bytes.NewBuffer([]byte(`{ "grantees": [
			"`+pubk2.String()+`",
			"`+pubk3.String()+`"
			]
		}`)))
		fileHis := file.HistroryAddress()
		err = upClient.AddActGrantees(ctx, &gFile, api.UploadOptions{BatchID: batchID, ActHistoryAddress: fileHis})
		if err != nil {
			return fmt.Errorf("node %s: add grantees error: %w", upNodeName, err)
		}
		c.logger.Info("ACT grantees added (grantee2, grantee3)")
		time.Sleep(10 * time.Second)

		// list grantees
		// ----------------------------------------------
		// Given the file is uploaded to the node (gFile)
		// When the grantees are listed
		// Then the grantees are listed successfully
		addresses, gErr := upClient.GetActGrantees(ctx, gFile.Address())
		if gErr != nil {
			return fmt.Errorf("node %s: GetActGrantees: %w", upNodeName, gErr)
		}
		if addresses == nil {
			return fmt.Errorf("node %s: GetActGrantees: addresses is nil", upNodeName)
		}
		if len(addresses) != 2 {
			return fmt.Errorf("node %s: GetActGrantees: addresses length is not 2", upNodeName)
		}
		c.logger.Info("ACT grantees listed")
		time.Sleep(10 * time.Second)

		// download act file with the publisher after create grantees
		// ----------------------------------------------
		// Given the grantee is added to the file
		// When the file is downloaded from the node with the publisher
		// Then the file is downloaded successfully
		h := gFile.HistroryAddress()
		size0, hash0, err0 := upClient.DownloadActFile(ctx, fileAddress, &api.DownloadOptions{Act: &act, ActPublicKey: &publisher, ActHistoryAddress: &h})
		if err0 != nil {
			return fmt.Errorf("node %s: %w", upNodeName, err0)
		}
		if !bytes.Equal(file.Hash(), hash0) {
			c.logger.Infof("Node %s. ACT file hash not equal. Uploaded size: %d Downloaded size: %d  File: %s", upNodeName, file.Size(), size0, fileAddress.String())
			return errors.New("ACT file retrieval - hash error")
		}
		c.logger.Info("ACT file downloaded with the publisher")
		time.Sleep(10 * time.Second)

		// download act file with the grantee2
		// ----------------------------------------------
		// Given the grantee2 is added to the file
		// When the file is downloaded from the node with the grantee2
		// Then the file is downloaded successfully
		his := gFile.HistroryAddress()
		sizeG2, hashG2, errG2 := client2.DownloadActFile(ctx, fileAddress, &api.DownloadOptions{Act: &act, ActPublicKey: &publisher, ActHistoryAddress: &his})
		if errG2 != nil {
			return fmt.Errorf("node %s: %w", nodeName2, errG2)
		}
		if !bytes.Equal(file.Hash(), hashG2) {
			c.logger.Infof("Node %s. ACT file hash not equal. Uploaded size: %d Downloaded size: %d  File: %s", nodeName2, file.Size(), sizeG2, fileAddress.String())
			return errors.New("ACT file retrieval - hash error")
		}
		c.logger.Info("ACT file downloaded with the grantee2")
		time.Sleep(10 * time.Second)

		// download act file with the grantee3
		// ----------------------------------------------
		// Given the grantee3 is added to the file
		// When the file is downloaded from the node with the grantee3
		// Then the file is downloaded successfully
		sizeG3, hashG3, errG3 := client3.DownloadActFile(ctx, fileAddress, &api.DownloadOptions{Act: &act, ActPublicKey: &publisher, ActHistoryAddress: &his})
		if errG3 != nil {
			return fmt.Errorf("node %s: %w", nodeName3, errG3)
		}
		if !bytes.Equal(file.Hash(), hashG3) {
			c.logger.Infof("Node %s. ACT file hash not equal. Uploaded size: %d Downloaded size: %d  File: %s", nodeName3, file.Size(), sizeG3, fileAddress.String())
			return errors.New("ACT file retrieval - hash error")
		}
		c.logger.Info("ACT file downloaded with the grantee3")
		time.Sleep(10 * time.Second)
	*/

	// patch grantees
	// ----------------------------------------------
	// Given the grantee is added to the file (gFile)
	// When the grantees are patched
	// Then the grantees are patched successfully
	/*
		pFile := bee.NewBufferFile("grantees-patch.json", bytes.NewBuffer([]byte(`{
					"add": [
						"`+pubk1.String()+`"
					],
					"revoke": [
						"`+pubk2.String()+`",
						"`+pubk3.String()+`"
					]
				}`)))
	*/
	pFile := bee.NewBufferFile("grantees-patch.json", bytes.NewBuffer([]byte(`{
		"add": [
			"`+pubk1.String()+`"
		],
		"revoke": []
	}`)))

	//pErr := upClient.PatchActGrantees(ctx, &pFile, gFile.Address(), gFile.HistroryAddress(), batchID)
	pErr := upClient.PatchActGrantees(ctx, &pFile, swarm.EmptyAddress, history, batchID)

	if pErr != nil {
		return fmt.Errorf("node %s: PatchActGrantees: %w", upNodeName, pErr)
	}
	c.logger.Info("ACT grantees patched add: grantee1, revoke: grantee2, grantee3")
	time.Sleep(10 * time.Second)
	/*

		// list grantees after patch
		// ----------------------------------------------
		// Given the grantee is patched
		// When the grantees are listed after patch
		// Then the grantees are listed successfully
		patchAddresses, patchErr := upClient.GetActGrantees(ctx, pFile.Address())
		if patchErr != nil {
			return fmt.Errorf("node %s: GetActGrantees after patch: %w", upNodeName, patchErr)
		}
		if patchAddresses == nil {
			return fmt.Errorf("node %s: GetActGrantees after patch: addresses is nil", upNodeName)
		}
		if len(patchAddresses) != 1 {
			return fmt.Errorf("node %s: GetActGrantees after patch: addresses length is not 1", upNodeName)
		}
		if patchAddresses[0] != pubk1.String() {
			return fmt.Errorf("node %s: GetActGrantees after patch: addresses is not equal to grantee1", upNodeName)
		}
		c.logger.Info("ACT grantees listed after patch")
		time.Sleep(10 * time.Second)

		// download act file with the not enabled grantee2 after patch
		//----------------------------------------------
		// Given the grantee is patched
		// When the file is downloaded from the node with the not enabled grantee2
		// Then the file download is denied
	*/
	hPatch := pFile.HistroryAddress()
	/*
		_, _, notG2Err := client2.DownloadActFile(ctx, fileAddress, &api.DownloadOptions{Act: &act, ActPublicKey: &publisher, ActHistoryAddress: &hPatch})
		if notG2Err == nil {
			return fmt.Errorf("node %s: File downloaded with wrong public key successfully - this is an error", nodeName2)
		}
		c.logger.Info("ACT Access denied for not enabled grantee2 after patch")
		time.Sleep(10 * time.Second)

		// download act file with the not enabled grantee3 after patch
		//----------------------------------------------
		// Given the grantee is patched
		// When the file is downloaded from the node with the not enabled grantee3
		// Then the file download is denied
		_, _, notG3Err := client3.DownloadActFile(ctx, fileAddress, &api.DownloadOptions{Act: &act, ActPublicKey: &publisher, ActHistoryAddress: &hPatch})
		if notG3Err == nil {
			return fmt.Errorf("node %s: File downloaded with wrong public key successfully - this is an error", nodeName3)
		}
		c.logger.Info("ACT Access denied for not enabled grantee3 after patch")
		time.Sleep(10 * time.Second)
	*/

	// download act file with the publisher after patch grantees
	// ----------------------------------------------
	// Given the grantee is patched
	// When the file is downloaded from the node with the publisher
	// Then the file is downloaded successfully
	sizeAfterPatch, hashAfterPatch, errAfterPatch := upClient.DownloadActFile(ctx, fileAddress, &api.DownloadOptions{Act: &act, ActPublicKey: &publisher, ActHistoryAddress: &hPatch})
	if errAfterPatch != nil {
		return fmt.Errorf("node %s: %w", upNodeName, errAfterPatch)
	}
	if !bytes.Equal(file.Hash(), hashAfterPatch) {
		c.logger.Infof("Node %s. ACT file hash not equal. Uploaded size: %d Downloaded size: %d  File: %s", upNodeName, file.Size(), sizeAfterPatch, fileAddress.String())
		return errors.New("ACT file retrieval - hash error")
	}
	c.logger.Info("ACT file downloaded with the publisher after patch")
	/*
		time.Sleep(10 * time.Second)

		// download act file with the added grantee1 after patch grantees
		// ----------------------------------------------
		// Given the grantee is patched
		// When the file is downloaded from the node with the grantee1
		// Then the file is downloaded successfully
		sizeAfterPatchG, hashAfterPatchG, errAfterPatchG := client1.DownloadActFile(ctx, fileAddress, &api.DownloadOptions{Act: &act, ActPublicKey: &publisher, ActHistoryAddress: &hPatch})
		if errAfterPatchG != nil {
			return fmt.Errorf("node %s: %w", nodeName1, errAfterPatchG)
		}
		if !bytes.Equal(file.Hash(), hashAfterPatchG) {
			c.logger.Infof("Node %s. ACT file hash not equal. Uploaded size: %d Downloaded size: %d  File: %s", nodeName1, file.Size(), sizeAfterPatchG, fileAddress.String())
			return errors.New("ACT file retrieval - hash error")
		}
		c.logger.Info("ACT file downloaded with the added grantee1 after patch")
	*/
	return
}
