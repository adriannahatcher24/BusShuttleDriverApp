import { ConnectionService } from './../Services/connection.service';
import { PreInspectionComponent } from './../pre-inspection/pre-inspection.component';
import { InspectionLog } from './../Models/inspectionLog';
import { InspectionService } from './../Services/inspection.service';
import { Component, OnInit } from '@angular/core';
import { Inspection } from './../Models/inspection-item';
import { Router } from '@angular/router';
import { AuthenticationService } from '../Services/authentication.service';
import { InspectionLogService } from './../Services/inspection-log.service';
import { trigger, state, style, transition, animate } from '@angular/animations';


@Component({
  selector: 'app-post-inspection',
  templateUrl: './post-inspection.component.html',
  styleUrls: ['./post-inspection.component.css'],
  animations: [
    trigger('simpleFadeAnimation', [
      state('in', style({ opacity: 1 })),
      transition(':enter', [
        style({ opacity: 0 }),
        animate(600)
      ]),
      transition(':leave',
        animate(600, style({ opacity: 0 })))
    ])
  ]
})
export class PostInspectionComponent implements OnInit {
  allItems = [];
  postItems = [];
  endMileage = '';
  endingHours = '';
  strItem = '';
  postComment = '';

  status = '';
  public onlineOffline: boolean = navigator.onLine;
  errMessage = 'Oops! There is no internet connection.';

  checkMileage;
  checkHours;
  errorMessageState = false;
  errorMessageStateHours = false;

  constructor(
    private inspecService: InspectionService,
    private router: Router,
    private authenticationService: AuthenticationService,
    private inspectionService: InspectionLogService,
    private connectionService: ConnectionService
  ) { }


  ngOnInit() {
    this.postItems = this.inspectionService.postItems;
    this.connectionService.createOnline$().subscribe(isOnline => this.onlineOffline = isOnline);
  }

  validateStartButton() {
    this.router.navigate(['/form']);
  }

  buttonState() {
    return !((this.postItems.every(_ => _.state)) && (this.endMileage !== '') && (this.endingHours !== '') );
  }

  onKey(event: any) { // without type info
    this.endMileage = event.target.value;
    if (this.validateMileage()) {
      this.errorMessageState = true;
    } else {
      this.errorMessageState = false;
     }
  }

  onHourKey(event: any) { // without type info
    this.endingHours = event.target.value;
    if (this.validateHours()) {
      this.errorMessageStateHours = true;
    } else {
      this.errorMessageStateHours = false;
     }
 }

 onCommentKey(event: any) { // without type info
  this.postComment = event.target.value;

}

  submitLog(): void {

      if (!this.onlineOffline) {
        this.errMessage = 'Oops! There is no internet connection.';
      } else {

         if (this.validateMileage()) {
          this.errorMessageState = true;
        } else {

            JSON.parse(localStorage.getItem('inspectionLogs'));
            this.createString();
            this.inspectionService.inspectionLog.endingMileage = this.endMileage;
            this.inspectionService.inspectionLog.endingHours = this.endingHours;
            this.inspectionService.inspectionLog.postInspectionComment = this.postComment;

            const copy = { ...this.inspectionService.inspectionLog }; // Creating a copy of the member 'log'.
            this.inspectionService.storeLogsLocally(copy);
            const aarayLastItem = this.inspectionService.inspectionToSend.length - 1;
            const inspectionLog = this.inspectionService.inspectionToSend[aarayLastItem];
            this.inspectionService.store(inspectionLog)
                    .subscribe((success) => {
                    localStorage.setItem('inspectionLogs', JSON.stringify(this.inspectionService.inspectionLog ));
                    });
            this.inspectionService.inspectionToSend = [];
            this.inspectionService.allItems = [];
            this.inspectionService.preItems = [];
            this.inspectionService.postItems = [];
            this.router.navigate(['/configure']);
            // Subscribing to the timer. If undo pressed, we unsubscribe.
      }
    }

  }

  validateMileage(): boolean {
    this.checkMileage = Number(this.endMileage);
    return isNaN(this.checkMileage);
  }

  validateHours(): boolean {
    this.checkHours = Number(this.endingHours);
    return isNaN(this.checkHours);
  }

  

  createString() {
    for (let i = 0 ;  i < this.postItems.length ; i++) {
      if ( i === this.postItems.length - 1 ) {
        this.strItem = this.strItem + '' + this.postItems[i].id;

      } else {
          this.strItem = this.strItem + '' + this.postItems[i].id + ',';
        }
      }
      this.inspectionService.inspectionLog.postInspection = this.strItem;
    }

}
