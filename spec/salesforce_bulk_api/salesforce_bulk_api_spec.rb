require 'spec_helper'
require 'yaml'
require 'restforce'

describe SalesforceBulkApi do

  before :each do
    auth_hash = YAML.load_file('auth_credentials.yml')
    sf_auth_hash = auth_hash['salesforce']

    @sf_client = Restforce.new(
      username: sf_auth_hash['user'],
      password: sf_auth_hash['passwordandtoken'],
      client_id: sf_auth_hash['client_id'],
      client_secret: sf_auth_hash['client_secret'],
      host: sf_auth_hash['host'])
    @sf_client.authenticate!

    @account_id = auth_hash['salesforce']['test_account_id']

    @api = SalesforceBulkApi::Api.new(@sf_client)
  end

  describe 'upsert' do

    context 'when not passed get_result' do
      it "doesn't return the batches array" do
        options = {
          external_field: 'Id'
        }
        res = @api.upsert('Account', [{:Id => @account_id, :Website => 'www.test.com'}], options)
        res['batches'].should be_nil
      end
    end

    context 'when passed get_response = true' do
      it 'returns the batches array' do
        options = {
          external_field: 'Id',
          get_response: true
        }
        res = @api.upsert('Account', [{:Id => @account_id, :Website => 'www.test.com'}], options)
        res['batches'][0]['response'].is_a? Array

        res['batches'][0]['response'][0]['id'].should start_with(@account_id)
        res['batches'][0]['response'][0]['success'].should eq 'true'
        res['batches'][0]['response'][0]['created'].should eq 'false'
      end
    end

    context 'when passed empty strings' do
      it 'sets the empty attributes' do
        options = {
          external_field: 'Id',
          get_response: true,
          nullable_fields: ['NumberOfEmployees']
        }
        @api.update('Account', [{:Id => @account_id, :Website => 'abc123', :Phone => '5678', :NumberOfEmployees => 10}], options)
        res = @api.query('Account', "SELECT Website, Phone, NumberOfEmployees From Account WHERE Id = '#{@account_id}'")
        res['batches'][0]['response'][0]['Website'].should eq 'abc123'
        res['batches'][0]['response'][0]['Phone'].should eq '5678'
        res['batches'][0]['response'][0]['NumberOfEmployees'].should eq '10'
        res = @api.upsert('Account', [{:Id => @account_id, :Website => ' ', :Phone => ' ', :NumberOfEmployees => nil}], options)
        res['batches'][0]['response'][0]['id'].should start_with(@account_id)
        res['batches'][0]['response'][0]['success'].should eq 'true'
        res['batches'][0]['response'][0]['created'].should eq 'false'
        res = @api.query('Account', "SELECT Website, Phone, NumberOfEmployees From Account WHERE Id = '#{@account_id}'")
        res['batches'][0]['response'][0]['Website'].should eq('')
        res['batches'][0]['response'][0]['Phone'].should eq('')
        res['batches'][0]['response'][0]['NumberOfEmployees'].should eq('')
      end
    end

  end

  describe 'update' do
    context 'when there is not an error' do
      context 'when not passed get_result' do
        it 'does not return the batches array' do
          res = @api.update('Account', [{:Id => @account_id, :Website => 'www.test.com'}])
          res['batches'].should be_nil
        end
      end

      context 'when passed get_result = true' do
        it 'returns the batches array' do
          res = @api.update('Account', [{:Id => @account_id, :Website => 'www.test.com'}], get_response: true)
          res['batches'][0]['response'].is_a? Array
          res['batches'][0]['response'][0]['id'].should start_with(@account_id)
          res['batches'][0]['response'][0]['success'].should eq 'true'
          res['batches'][0]['response'][0]['created'].should eq 'false'
        end
      end
    end

    context 'when there is an error' do
      context 'when not passed get_result' do
        it 'does not return the results array' do
          res = @api.update('Account', [
            {:Id => @account_id, :Website => 'www.test.com'},
            {:Id => 'abc123', :Website => 'www.test.com'}
          ])
          res['batches'].should be_nil
        end
      end

      context 'when passed get_result = true with batches' do
        it 'returns the results array' do
          res = @api.update(
            'Account',
            [
              {:Id => @account_id, :Website => 'www.test.com'},
              {:Id => @account_id, :Website => 'www.test.com'},
              {:Id => @account_id, :Website => 'www.test.com'},
              {:Id => 'abc123', :Website => 'www.test.com'}
            ],
            get_response: true,
            batch_size: 2
          )

          res['batches'][0]['response'][0]['id'].should start_with(@account_id)
          res['batches'][0]['response'][0]['success'].should eq 'true'
          res['batches'][0]['response'][0]['created'].should eq 'false'
          res['batches'][0]['response'][1]['id'].should start_with(@account_id)
          res['batches'][0]['response'][1]['success'].should eq 'true'
          res['batches'][0]['response'][1]['created'].should eq 'false'

          res['batches'][1]['response'][0]['id'].should start_with(@account_id)
          res['batches'][1]['response'][0]['success'].should eq 'true'
          res['batches'][1]['response'][0]['created'].should eq 'false'
          res['batches'][1]['response'][1].should eq({
            'error' => 'MALFORMED_ID:Account ID: id value of incorrect type: abc123:Id --',
            'success' => 'false',
            'created' => 'false',
            'id' => ''
          })
        end
      end
    end

  end

  describe 'create' do
    pending
  end

  describe 'delete' do
    pending
  end

  describe 'query' do

    context 'when there are results' do
      context 'and there are multiple batches' do
        # need dev to create > 10k records in dev organization
        it 'returns the query results in a merged hash'
      end
    end

    context 'when there are no results' do
      it 'returns nil' do
        res = @api.query('Account', "SELECT id From Account WHERE Name = 'ABC'",)
        res['batches'][0]['response'].should eq []
      end
    end

    context 'when there is an error' do
      it 'returns nil' do
        res = @api.query('Account', "SELECT id From Account WHERE Name = ''ABC'")
        res['batches'][0]['response'].should eq nil
      end
    end

  end

  describe 'counters' do
    context 'when read operations are called' do
      it 'increments operation count and http GET count' do
        @api.counters[:http_get].should eq 0
        @api.counters[:query].should eq 0
        @api.query('Account', "SELECT Website, Phone From Account WHERE Id = '#{@account_id}'", get_response: false)
        @api.counters[:http_post].should eq 3
        @api.counters[:http_get].should eq 0
        @api.counters[:query].should eq 1
      end
    end

    context 'when update operations are called' do
      it 'increments operation count and http POST count' do
        @api.counters[:http_post].should eq 0
        @api.counters[:update].should eq 0
        @api.update('Account', [{:Id => @account_id, :Website => 'abc123', :Phone => '5678'}], get_response: false)
        @api.counters[:http_post].should eq 3
        @api.counters[:update].should eq 1
      end
    end
  end

end
