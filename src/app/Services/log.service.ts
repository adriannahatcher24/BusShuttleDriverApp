import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { retryWhen, catchError, retry } from 'rxjs/operators';
import { Observable, throwError, timer, BehaviorSubject } from 'rxjs';
import { mergeMap, finalize } from 'rxjs/operators';
import { Log } from '../Models/log';
import { Stop } from '../Models/stop';
import { Loop } from '../Models/loop';
import { environment } from '../../environments/environment';
import { ConnectionService } from './../Services/connection.service';

@Injectable({
  providedIn: 'root'
})
export class LogService {
  baseUrl = environment.BASE_API_URL;
  logsToSend: Log[] = [];
  stops: Stop[];
  loops: Loop[];
  isSyncing: boolean;
  public onlineOffline: boolean = navigator.onLine;

  private syncMessageSource = new BehaviorSubject<string>('All done! Have a wonderful day!');
  currentSyncMessage = this.syncMessageSource.asObservable();

  private syncCountSource = new BehaviorSubject<number>(0);
  currentSyncCount = this.syncCountSource.asObservable();

  constructor(private http: HttpClient, private connectionService: ConnectionService) {
    const logs: Log[] = JSON.parse(localStorage.getItem('logs'));
    if (logs !== null) {
      this.logsToSend = logs;
      this.changeSyncCount(logs.length);
    }
  }

  changeSyncMessage(message: string) {
    if (message === 'syncStarted') {
      this.syncMessageSource.next('Syncing, please do not close this page.');
    } if (message === 'syncDone') {
      this.syncMessageSource.next('All done! Have a wonderful day!');
    } if (message === 'syncError') {
      this.syncMessageSource.next('There was an error. Please ensure you have a stable WiFi connection and try again.');
    } if (message === 'initialSyncState') {
      this.syncMessageSource.next('All done! Have a wonderful day!');
    } if (message === 'noInternet') {
      this.syncMessageSource.next('There was an error. Please ensure you have a stable WiFi connection and try again.');
    }
  }

  getSyncingStatus(): boolean {
    return this.isSyncing;
  }

  changeSyncCount(count: number) {
    this.syncCountSource.next(count);
  }

  storeLogsLocally(log: Log) {
    this.logsToSend.push(log);
    localStorage.setItem('logs', JSON.stringify(this.logsToSend));
    if (this.logsToSend === null) {
      return;
    } else {
      this.changeSyncCount(this.logsToSend.length);
    }
  }


  public syncLogs(): void {
    console.log('syncLogs Initiated');
    this.isSyncing = true;
    if (this.logsToSend === null || this.logsToSend.length === 0 || this.logsToSend === undefined) {
      this.changeSyncMessage('syncDone');
      this.isSyncing = false;
      return;
    }
    this.changeSyncMessage('syncStarted');

    if ('onLine' in navigator) {
      if (!navigator.onLine) {
        console.log('offline');
        this.changeSyncMessage('noInternet');
        this.isSyncing = false;
      } else {
        if (this.logsToSend.length > 0) {
          this.isSyncing = true;
          for (let i = this.logsToSend.length - 1; i >= 0; i--) {
            const log = this.logsToSend[i];
            this.store(log)
              .subscribe((success) => {
                // console.log(success);
                this.logsToSend.pop();
                localStorage.setItem('logs', JSON.stringify(this.logsToSend));
                this.changeSyncCount(this.logsToSend.length);
                if (this.logsToSend.length === 0) {
                  this.changeSyncMessage('syncDone');
                  this.isSyncing = false;
                  localStorage.removeItem('logs');
                  this.logsToSend = [];
                }
              },
                (err: any) => {
                  this.isSyncing = false;
                  this.changeSyncMessage('syncError');
                });
          }
        }
      }
    }

  }

  directSubmit(log: Log): Observable<Log> {
    return this.http.post<Log>(this.baseUrl + '/store.php', { data: log });
  }

  store(log: Log): Observable<Log> {
    return this.http.post<Log>(this.baseUrl + '/store.php', { data: log })
      .pipe(
        retryWhen(this.generateRetryStrategy(log)({
          scalingDuratinion: 500,
          excludedStatusCodes: [500]
        })),
        catchError(this.handleError));
  }

  private generateRetryStrategy(log: Log) {
    const retryStrategy = ({
      maxRetryAttempts = 2,
      scalingDuratinion = 500,
      
      excludedStatusCodes = []
    }: {
      maxRetryAttempts?: number,
      scalingDuratinion?: number,
      excludedStatusCodes?: number[]
    } = {}) => (attempts: Observable<any>) => {
      return attempts.pipe(
        mergeMap((error, i) => {
          const retryAttempt = i + 1;
          // if maximum number of retries have been met
          // or response is a status code we don't wish to retry, throw error
          if (
            retryAttempt === maxRetryAttempts ||
            excludedStatusCodes.find(e => e === error.status)
          ) {
            return this.handleError(error);
          }
          console.log( // uncomment for demonstration
            `Attempt ${retryAttempt}: retrying in ${retryAttempt *
            scalingDuratinion}ms`
          );
          return timer(retryAttempt * scalingDuratinion);
        }),
        // finalize(() => (console.log('Error! something went wrong.')))
      );
    };
    return retryStrategy;
  }

  private handleError(error: HttpErrorResponse) {
    return throwError('Error! something went wrong.');
  }
}
