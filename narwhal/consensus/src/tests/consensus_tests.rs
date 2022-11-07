// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::mutable_key_type)]

use fastcrypto::hash::Hash;
use node::NodeStorage;
use prometheus::Registry;
use std::collections::BTreeSet;
use std::sync::Arc;
use telemetry_subscribers::TelemetryGuards;
use test_utils::{temp_dir, CommitteeFixture};
use tokio::sync::watch;

use crate::bullshark::Bullshark;
use crate::metrics::ConsensusMetrics;
use crate::Consensus;
use types::{Certificate, ReconfigureNotification};

#[tokio::test]
async fn test_consensus_recovery() {
    let _guard = setup_tracing();

    // GIVEN
    let storage = NodeStorage::reopen(temp_dir());

    let consensus_store = storage.consensus_store;
    let certificate_store = storage.certificate_store;

    // AND Setup consensus
    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();

    // AND Make certificates for rounds 1 to 4. We expect the certificates on round 3
    // to trigger a commit
    let keys: Vec<_> = fixture.authorities().map(|a| a.public_key()).collect();
    let genesis = Certificate::genesis(&committee)
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, next_parents) =
        test_utils::make_optimal_certificates(&committee, 1..=4, &genesis, &keys);

    // AND Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = test_utils::test_channel!(100);
    let (tx_primary, _rx_primary) = test_utils::test_channel!(100);
    let (tx_output, mut rx_output) = test_utils::test_channel!(1);

    let initial_committee = ReconfigureNotification::NewEpoch(committee.clone());
    let (_tx_reconfigure, rx_reconfigure) = watch::channel(initial_committee.clone());

    let gc_depth = 50;
    let metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
    let bullshark = Bullshark::new(
        committee.clone(),
        consensus_store.clone(),
        gc_depth,
        metrics.clone(),
    );

    let consensus_handle = Consensus::spawn(
        committee.clone(),
        consensus_store.clone(),
        certificate_store.clone(),
        rx_reconfigure,
        rx_waiter,
        tx_primary,
        tx_output,
        bullshark,
        metrics.clone(),
        gc_depth,
    );

    // WHEN we feed all certificates to the consensus.
    while let Some(certificate) = certificates.pop_front() {
        // we store the certificates so we can enable the recovery
        // mechanism later.
        certificate_store.write(certificate.clone()).unwrap();
        tx_waiter.send(certificate).await.unwrap();
    }

    // THEN we expect the first 4 ordered certificates to be from round 1 (they are the parents of the committed
    // leader) and the last committed to be the leader of round 2
    let mut consensus_index_counter = 0;
    let num_of_committed_certificates = 5;

    for i in 1..=num_of_committed_certificates {
        let output = rx_output.recv().await.unwrap();
        assert_eq!(output.consensus_index, consensus_index_counter);

        if i < 5 {
            assert_eq!(output.certificate.round(), 1);
        } else {
            assert_eq!(output.certificate.round(), 2);
        }

        consensus_index_counter += 1;
    }

    // AND the last committed store should be updated correctly
    // For the leader of round 2 we expect to have last committed record of 2.
    // For the others should be 1.
    let last_committed = consensus_store.read_last_committed();

    for key in keys.clone() {
        let last_round = *last_committed.get(&key).unwrap();

        if key == Bullshark::leader_authority(&committee, 2) {
            assert_eq!(last_round, 2);
        } else {
            assert_eq!(last_round, 1);
        }
    }

    // AND shutdown consensus
    consensus_handle.abort();

    // AND bring up consensus again
    let (_tx_reconfigure, rx_reconfigure) = watch::channel(initial_committee);
    let (tx_waiter, rx_waiter) = test_utils::test_channel!(100);
    let (tx_primary, _rx_primary) = test_utils::test_channel!(100);
    let (tx_output, mut rx_output) = test_utils::test_channel!(1);

    let bullshark = Bullshark::new(
        committee.clone(),
        consensus_store.clone(),
        gc_depth,
        metrics.clone(),
    );

    let _consensus_handle = Consensus::spawn(
        committee.clone(),
        consensus_store.clone(),
        certificate_store.clone(),
        rx_reconfigure,
        rx_waiter,
        tx_primary,
        tx_output,
        bullshark,
        metrics.clone(),
        gc_depth,
    );

    // AND create certificates at round 5. They should trigger a leader election
    // for round 4 and commit anything before. However, no certificate from round < 2
    // should be committed.
    for key in keys {
        let (_, certificate) =
            test_utils::mock_certificate(&committee, key.clone(), 5, next_parents.clone());
        certificates.push_back(certificate);
    }

    // WHEN we feed all certificates of round 5 to the consensus.
    while let Some(certificate) = certificates.pop_front() {
        // we store the certificates so we can enable the recovery
        // mechanism later.
        certificate_store.write(certificate.clone()).unwrap();
        tx_waiter.send(certificate).await.unwrap();
    }

    // THEN we expect to commit with a leader of round 4. Assuming a correct Consensus
    // restore , we should not commit any certificate of index < 2.
    // We expect to have committed in total:
    // * 3 certificates of round 2 (since we already committed earlier its leader)
    // * 4 certificates of round 3 (the parents of the leader)
    // * 1 certificate of round 4 (the leader of the round)
    //
    // so totally 3 + 4 + 1 = 8 certificates
    //
    // Ensure committed certificates are all over the last committed rounds.
    // Should continue from the last consensus index.
    while let Some(output) = rx_output.recv().await {
        assert_eq!(output.consensus_index, consensus_index_counter);

        let last_committed_round = last_committed
            .get(&output.certificate.header.author)
            .unwrap();
        assert!(output.certificate.round() > *last_committed_round);

        // we don't expect to see another committed certificate after that,
        // so now just break
        if output.certificate.round() == 4 {
            break;
        }

        consensus_index_counter += 1;
    }

    // We expect to have committed:
    // * 5 certificates before the consensus restart
    // * 8 certificates after the consensus restart
    // the index is zero based, so in the end we expect to see that we have assigned
    // to the latest certificate the index 12.
    assert_eq!(consensus_index_counter, 12);
}

fn setup_tracing() -> TelemetryGuards {
    // Setup tracing
    let tracing_level = "debug";
    let network_tracing_level = "info";

    let log_filter = format!("{tracing_level},h2={network_tracing_level},tower={network_tracing_level},hyper={network_tracing_level},tonic::transport={network_tracing_level}");

    telemetry_subscribers::TelemetryConfig::new("narwhal")
        // load env variables
        .with_env()
        // load special log filter
        .with_log_level(&log_filter)
        .init()
        .0
}
