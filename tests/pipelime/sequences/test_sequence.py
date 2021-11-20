from pipelime.sequences.samples import SamplesSequence


class TestSequenceNormalized(object):
    def test_normalized(self, plain_samples_sequence_generator):

        N = 100
        dataset: SamplesSequence = plain_samples_sequence_generator("d{idx}_", N)
        assert dataset.is_normalized()

        # Changes all keys change should be normalized as well
        dataset: SamplesSequence = plain_samples_sequence_generator("d{idx}_", N)
        for i in range(N):
            dataset[i]["#IPOSSsibl3!KEY_@0123"] = {"fake_data": f"d_{i}"}
        assert dataset.is_normalized()

        # Changes All keys but last should be not normalized
        dataset: SamplesSequence = plain_samples_sequence_generator("d{idx}_", N)
        for i in range(N - 2):
            dataset[i]["#IPOSSsibl3!KEY_@0123"] = {"fake_data": f"d_{i}"}
        assert not dataset.is_normalized()

        # Changes All keys but first should be not normalized
        dataset: SamplesSequence = plain_samples_sequence_generator("d{idx}_", N)
        for i in range(1, N):
            dataset[i]["#IPOSSsibl3!KEY_@0123"] = {"fake_data": f"d_{0}"}
        assert not dataset.is_normalized()

        # Changes first key should be not normalized
        dataset: SamplesSequence = plain_samples_sequence_generator("d{idx}_", N)
        for i in range(1):
            dataset[i]["#IPOSSsibl3!KEY_@0123"] = {"fake_data": f"d_{0}"}
        assert not dataset.is_normalized()
