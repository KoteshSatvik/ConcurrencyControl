#include<bits/stdc++.h>
#include <unistd.h>
#include<sys/time.h>
using namespace std;
// Maintaining global variables
// M -> no. of data items; N -> no. of threads
// global counter variable
int numTrans, numThreads;
int n, m, constVal;
double l;
default_random_engine generator;
exponential_distribution<double> distribution(l);
float tot_aborts = 0.0;
float time_com = 0.0;
pthread_mutex_t counter_lock, lock5;
int counter_id = 0;
ofstream logFile;

// Struct for data items
struct dataItem{
    int id;                 // id of the data item
    int val;                // value of the data item
    vector<int> rList;      // list of transactions that have read the data item
    vector<int> wList;      // list of transactions that have written the data item
    pthread_mutex_t lock;   // lock for the data item
    map<int, int> depend;
};

// Class for transaction object
class transaction{
    public:
        int id;                 // id of the transaction
        int status;             // 0-> live, 1-> commit, 2-> abort
        set<int> rSet;       // set of data items read by the transaction
        map<int, int> wSet;       // set of data items written by the transaction
        struct timeval start, end;
        map<int, int> depend;
        map<int, dataItem*> loc_copy;
        transaction(int id){
            this->id = id;
            this->status = 0;
            for(int i=0; i<m; i++){
                this->depend.insert(make_pair(i, -1));//not even the 0th write has been in this, hence -1
            }
            gettimeofday(&this->start, NULL);
        }
};

float subrtime(struct timeval *x, struct timeval *y){
    float z;
    z = (float) (x->tv_sec - y->tv_sec) + (float) (x->tv_usec - y->tv_usec)/1000000;
    return z;
}

string convT(time_t epoch_time){
	tm *t = localtime(&epoch_time);
	string ct = to_string(t->tm_hour)+":"+to_string(t->tm_min)+":"+to_string(t->tm_sec);
	return ct;
}

map<int, dataItem*> shared;        // vector of data items
map<int, transaction*> tx;      // map of transaction objects

transaction* begin_trans(){
	// setting a local vble value to the counter
	pthread_mutex_lock(&counter_lock);
	counter_id++;
	int id_val = counter_id;
	pthread_mutex_unlock(&counter_lock);
	//initilalize a rSet and wSet
    transaction* new_t = new transaction(id_val);
    tx.insert(pair<int, transaction*>(id_val, new_t));
	return new_t;
}

void read_t(int id, int dt_id, int* loc){
    dataItem* data = shared[dt_id];
    transaction* t = tx[id];

    if(t->loc_copy.find(dt_id) != t->loc_copy.end()){
        *loc = (t->loc_copy[dt_id])->val;
        return;
    }

    dataItem* tp = shared[dt_id];
    t->loc_copy.insert(make_pair(dt_id, tp));
    *loc = data->val;

    t->rSet.insert(dt_id);

    t->depend[dt_id] = (t->loc_copy[dt_id])->depend[dt_id];

    for(auto e : t->rSet){
        if(t->depend[e] < (t->loc_copy[dt_id])->depend[e]){
            t->status = 2;
            return;
        }
    }

    for(int i=0; i<m; i++){
        if(t->rSet.find(i) != t->rSet.end()){
            continue;
        }
        t->depend[i] = max(t->depend[i], (t->loc_copy[dt_id])->depend[i]);
    }

    *loc = (t->loc_copy[dt_id])->val;
    return;
    pthread_mutex_lock(&data->lock);
    data->rList.push_back(id);
    pthread_mutex_unlock(&data->lock);
}

void write_t(int id, int dt_id, int val){
    transaction* t = tx[id];
    
    if(t->loc_copy.find(dt_id) == t->loc_copy.end()){
        dataItem* tp = shared[dt_id];
        t->loc_copy.insert(make_pair(dt_id, tp));
    }
    t->loc_copy[dt_id]->val = val;
    t->wSet[dt_id] = val;
}

void tryC(int id){
    transaction* t = tx[id];
    int chk = 0;
    if(t->status == 2){
        return;
    }
    for(auto e : t->rSet){
        dataItem* data = shared[e];
        if(t->depend[e] != data->depend[e]){
            chk = 1;
            break;
        }
    }
    //locking all the data items in rSet and wSet of the transaction
    for(auto e : t->rSet){
        dataItem* data = shared[e];
        pthread_mutex_lock(&data->lock);
    }
    for(auto e : t->wSet){
        dataItem* data = shared[e.first];
        pthread_mutex_lock(&data->lock);
    }
    if(t->rSet.size() != 0){
        if(chk == 1){
            t->status = 2;
            for(auto e : t->rSet){
                dataItem* data = shared[e];
                pthread_mutex_unlock(&data->lock);
            }
            for(auto e : t->wSet){
                dataItem* data = shared[e.first];
                pthread_mutex_unlock(&data->lock);
            }
            return;
        }
    }
    if(t->wSet.size() != 0){
        for(auto e : t->wSet){
            dataItem* data = shared[e.first];
            t->depend[e.first] = data->depend[e.first] + 1;
        }
        for(auto e:t->wSet){
            shared[e.first]->val = t->loc_copy[e.first]->val;
            shared[e.first]->depend[e.first] = t->depend[e.first];
        }
    }
    // releasing all the locks
    for(auto e : t->rSet){
        dataItem* data = shared[e];
        pthread_mutex_unlock(&data->lock);
    }
    for(auto e : t->wSet){
        dataItem* data = shared[e.first];
        pthread_mutex_unlock(&data->lock);
    }
    t->status = 1;
    return;
}

void* updtMem(void* arg){
    int status = 2;     //0->live, 1->commited, 2->aborted
	int abortCnt = 0;
	struct timeval critStartTime, critEndTime;
	for(int curTrans = 0; curTrans<n ; curTrans ++){
		abortCnt = 0;
        gettimeofday(&critStartTime, NULL);
		do{
			transaction* t = begin_trans();
			int randIters = rand()%m;
			int locVal;
			for(int i=0; i<randIters; i++){
				int randInd = rand()%m;
				int randVal = rand()%constVal;
                read_t(t->id, randInd, &locVal);
                struct timeval readTime, writeTime;	
                gettimeofday(&readTime, NULL);
                pthread_mutex_lock(&lock5);			
				logFile << "Thread id " << pthread_self() << " Transaction " << t->id << " reads from " << randInd  << " a value " << locVal << " at time " << convT(readTime.tv_sec) <<"\n";
                pthread_mutex_unlock(&lock5);			
                locVal += randVal;
				write_t(t->id, randInd, locVal);
                gettimeofday(&writeTime, NULL);
                pthread_mutex_lock(&lock5);
				logFile << "Thread id " << pthread_self() << " Transaction "<< t->id << " writes to " << randInd  << " a value " << locVal << " at time " << convT(writeTime.tv_sec) <<"\n";
				pthread_mutex_unlock(&lock5);
                float randTime = distribution(generator);
				usleep(randTime*1000000);
			}
            struct timeval commitTime;
            gettimeofday(&commitTime, NULL);
            tryC(t->id);
            pthread_mutex_lock(&lock5);
            logFile << "Transaction" << t->id << " tryCommits with result " << t->status  << " at time " <<convT(commitTime.tv_sec)<<"\n";
            if(t->status == 2){
                status = 2;
            }
            else{
                status = 1;
            }
            pthread_mutex_unlock(&lock5);
            abortCnt++;
		}
		while(status != 1);
        cout << "check-out" << "\n";
		gettimeofday(&critEndTime,NULL);
        pthread_mutex_lock(&lock5);
        tot_aborts += abortCnt;
        time_com += critEndTime.tv_sec - critStartTime.tv_sec + critEndTime.tv_usec/1000000.0 - critStartTime.tv_usec/1000000.0;
		cout << "commit time value : " << critEndTime.tv_sec - critStartTime.tv_sec + critEndTime.tv_usec/1000000.0 - critStartTime.tv_usec/1000000.0 << " for thread " << pthread_self() << endl;
        pthread_mutex_unlock(&lock5);
	}
}

int main(){
    ifstream ips;
    ips.open("inp-params.txt");
    ips >> numThreads >> m >> numTrans >> constVal >> l;
    n = numTrans/numThreads;
    logFile.open("vw_cons-log.txt");
    // Initializing the data items
    for(int i=0; i<m; i++){
        dataItem* temp = (dataItem*)malloc(sizeof(dataItem));
        temp->id = i;
        temp->val = 0;
        for(int j=0; j<m; j++){
            temp->depend.insert(make_pair(j, 0));
        }
        // temp->id_ver.insert(make_pair(0, 0));
        pthread_mutex_init(&temp->lock, NULL);
        shared.insert(pair<int, dataItem*>(i, temp));
    } 
    pthread_t threads[numThreads];
    pthread_mutex_init(&counter_lock, NULL);
    pthread_mutex_init(&lock5, NULL);
    for(int j=0; j<numThreads; j++){
        pthread_create(&threads[j], NULL, updtMem, NULL);
    }
    for(int k=0; k<numThreads; k++){
        pthread_join(threads[k], NULL);
    }
    cout << "Average time taken for each transaction to commit is " << time_com/(float)numTrans << " seconds.";
    cout << "Average number of aborts are: " << tot_aborts/(float)numTrans << ".";
    logFile.close();
    return 0;
}